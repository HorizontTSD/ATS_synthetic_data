from src.db_clients.clients import test_get_db_connection

conn = test_get_db_connection()

cursor = conn.cursor()

tables = {
    "electrical_consumption_arkhangelskaya_obl": """
        CREATE TABLE electrical_consumption_arkhangelskaya_obl (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            datetime DATETIME NOT NULL,
            day_zone TEXT,
            dSO_GP REAL,
            VC_PPP REAL,
            VC_fact REAL,
            I_ee_ph REAL,
            I_em_ph REAL,
            I_otkl_ph REAL
        )
    """,
    "electrical_consumption_amurskaya_obl": """
        CREATE TABLE electrical_consumption_amurskaya_obl (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            datetime DATETIME NOT NULL,
            day_zone TEXT,
            dSO_GP REAL,
            VC_PPP REAL,
            VC_fact REAL,
            I_ee_ph REAL,
            I_em_ph REAL,
            I_otkl_ph REAL
        )
    """,
    "electrical_consumption_evreyskaya_obl": """
        CREATE TABLE electrical_consumption_evreyskaya_obl (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            datetime DATETIME NOT NULL,
            day_zone TEXT,
            dSO_GP REAL,
            VC_PPP REAL,
            VC_fact REAL,
            I_ee_ph REAL,
            I_em_ph REAL,
            I_otkl_ph REAL
        )
    """
}

for table_name, ddl in tables.items():
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    print("="*75)
    print(f"Таблица '{table_name}' удалена (если существовала)")
    cursor.execute(ddl)
    print(f"Таблица '{table_name}' создана")

conn.commit()
conn.close()
