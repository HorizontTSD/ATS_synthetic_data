import os
import time
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from tqdm import tqdm
from src.db_clients.clients import get_db_connection

home_path = os.getcwd()
path_to_save_synthetic_data = os.path.join(home_path, "src", "data", "ats_data_synthetic")
default_start_date = "2020-08-21 08:00:00"
BATCH_SIZE = 300

def load_synthetic_data():
    try:
        df_ark = pd.read_csv(os.path.join(path_to_save_synthetic_data, "arkhangelsk_obl_synthetic.csv"))
        df_amur = pd.read_csv(os.path.join(path_to_save_synthetic_data, "amurskaya_obl_synthetic.csv"))
        df_evr = pd.read_csv(os.path.join(path_to_save_synthetic_data, "evreyskaya_obl_synthetic.csv"))
        return {
            "electrical_consumption_evreyskaya_obl": df_evr,
            "electrical_consumption_amurskaya_obl": df_amur,
            "electrical_consumption_arkhangelskaya_obl": df_ark
        }
    except Exception as e:
        print(f"[{datetime.now()}] Ошибка при загрузке CSV: {e}")
        raise

dict_dfs = load_synthetic_data()

def get_last_datetime_query(table_name: str) -> str:
    return f"SELECT MAX(datetime) AS last_datetime FROM {table_name};"

def insert_data(conn, table_name: str, df: pd.DataFrame) -> int:
    cursor = conn.cursor()
    column_mapping = {
        "dSO_ГП": "dSO_GP",
        "VC_ППП": "VC_PPP",
        "VC_факт": "VC_fact",
        "I_ээ ph": "I_ee_ph",
        "I_эм ph": "I_em_ph",
        "I_откл ph": "I_otkl_ph",
    }
    df = df.rename(columns=column_mapping)
    df["datetime"] = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    rows_added = 0
    try:
        for start in tqdm(range(0, len(df), BATCH_SIZE), desc=f"Inserting into {table_name}", unit="batch"):
            batch = df.iloc[start:start+BATCH_SIZE]
            cursor.executemany(f"""
                INSERT INTO {table_name} (datetime, day_zone, dSO_GP, VC_PPP, VC_fact, I_ee_ph, I_em_ph, I_otkl_ph)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, batch[["datetime", "day_zone", "dSO_GP", "VC_PPP", "VC_fact", "I_ee_ph", "I_em_ph", "I_otkl_ph"]].values.tolist())
            conn.commit()
            rows_added += len(batch)
        print(f"[{datetime.now()}] Вставлено {rows_added} строк в {table_name}")
    except Exception as e:
        print(f"[{datetime.now()}] Ошибка при вставке данных в {table_name}: {e}")
    return rows_added

def refresh_and_append():
    moscow_tz = ZoneInfo("Europe/Moscow")
    summary = []

    for table, df in dict_dfs.items():
        rows_added = 0
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            try:
                cursor.execute(get_last_datetime_query(table))
                last_time_in_table = cursor.fetchone()[0]
            except Exception:
                last_time_in_table = None

            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            if df["datetime"].dt.tz is None:
                df["datetime"] = df["datetime"].dt.tz_localize(moscow_tz)
            df["datetime"] = df["datetime"].dt.tz_convert(moscow_tz).dt.tz_localize(None)

            start_date = pd.to_datetime(default_start_date) if last_time_in_table is None else pd.to_datetime(last_time_in_table)
            start_date = start_date.tz_localize(moscow_tz).tz_convert(moscow_tz).tz_localize(None)
            moscow_time = datetime.now(moscow_tz).replace(tzinfo=None)

            df_to_write = df[(df["datetime"] > start_date) & (df["datetime"] <= moscow_time)]

            if not df_to_write.empty:
                rows_added = insert_data(conn, table, df_to_write)

        except Exception as e:
            summary.append(f"{table}: ERROR ({e})")
        finally:
            conn.close()

        summary.append(f"{table}: {rows_added} rows added")

    print(f"[{datetime.now()}] Итог по всем таблицам: " + " | ".join(summary))

def run_forever(interval_seconds: int = 60):
    print(f"[{datetime.now()}] run_forever запущен, интервал: {interval_seconds} секунд")
    while True:
        try:
            refresh_and_append()
        except Exception as e:
            print(f"[{datetime.now()}] Ошибка в run_forever: {e}")
        time.sleep(interval_seconds)
