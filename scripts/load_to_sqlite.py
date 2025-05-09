import sqlite3

import pandas as pd


def load_csv_to_sqlite(csv_path, db_path, table_name):
    df = pd.read_csv(csv_path)
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Данные из {csv_path} успешно загружены в таблицу '{table_name}' в базу {db_path}")


if __name__ == "__main__":
    csv_path = "data/processed/titanic_processed.csv"
    db_path = "data/db/titanic.db"
    table_name = "titanic_data"

    load_csv_to_sqlite(csv_path, db_path, table_name)
