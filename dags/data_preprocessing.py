from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sqlite3
import pandas as pd
import os
from time import time
from typing import Tuple

ROOT_TASK_DATA = "/opt/airflow/task_data"
DB_PATH = f"{ROOT_TASK_DATA}/db/titanic.db"
CSV_SOURCE = f"{ROOT_TASK_DATA}/raw/titanic.csv"
PROCESSED_DIR = f"{ROOT_TASK_DATA}/processed"

RAW_TABLE = "raw_data"
PREPROCESSED_TABLE = "preprocessed_data"

def append_csv_to_sqlite():
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    
    if os.path.exists(CSV_SOURCE):
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_csv(CSV_SOURCE)
        df.to_sql(RAW_TABLE, conn, if_exists="replace", index=False)
        conn.close()
        
        timestamp = int(time())
        processed_path = os.path.join(PROCESSED_DIR, f"increment_done_{timestamp}.csv")
        os.rename(CSV_SOURCE, processed_path)
        return True
    return False

def preprocess_data(**kwargs) -> pd.DataFrame:
    ti = kwargs['ti']
    file_processed = ti.xcom_pull(task_ids='append_csv_to_sqlite')
    
    if not file_processed:
        raise ValueError("No new data to process")
    
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"SELECT * FROM {RAW_TABLE}", conn)
    conn.close()
    
    df = df.drop(['Name', 'Ticket', 'Cabin', 'PassengerId'], axis=1, errors='ignore')
    df['Age'] = df['Age'].fillna(df['Age'].median())
    df = pd.get_dummies(df, columns=['Sex'], drop_first=True)
    
    return df

def save_preprocessed_data(**kwargs):
    ti = kwargs['ti']
    df_preprocessed = ti.xcom_pull(task_ids='preprocess_data')
    
    conn = sqlite3.connect(DB_PATH)
    df_preprocessed.to_sql(PREPROCESSED_TABLE, conn, if_exists="append", index=False)
    conn.close()
    print(f"Saved {len(df_preprocessed)} preprocessed rows to {PREPROCESSED_TABLE}")

with DAG(
    "split_preprocessing",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    append_task = PythonOperator(
        task_id="append_csv_to_sqlite",
        python_callable=append_csv_to_sqlite,
    )
    
    preprocess_task = PythonOperator(
        task_id="preprocess_data",
        python_callable=preprocess_data,
    )
    
    save_task = PythonOperator(
        task_id="save_preprocessed_data",
        python_callable=save_preprocessed_data,
    )
    
    append_task >> preprocess_task >> save_task