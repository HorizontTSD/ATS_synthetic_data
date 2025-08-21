import pandas as pd
from src.db_clients.clients import test_get_db_connection

def load_all_data():
    conn = test_get_db_connection()

    table_names = [
        "electrical_consumption_evreyskaya_obl",
        "electrical_consumption_amurskaya_obl",
        "electrical_consumption_arkhangelskaya_obl"
    ]

    dict_dfs = {}

    for table in table_names:
        query = f"SELECT * FROM {table};"
        df = pd.read_sql_query(query, conn)
        dict_dfs[table] = df

    conn.close()
    return dict_dfs


if __name__ == "__main__":
    dict_dfs = load_all_data()
    for table, df in dict_dfs.items():
        print("=" * 80)
        print(f"Таблица: {table}, строк: {len(df)}")
        print(df.head())
