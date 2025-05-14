from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import sqlite3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_csv_to_sqlite(csv_path, db_path, table_name):
    df = pd.read_csv(csv_path)
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    print(f"Data from {csv_path} successfully loaded to table '{table_name}' in database {db_path}")

def preprocess(input_table: str, output_table: str, db_path: str):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(f"SELECT * FROM {input_table}", conn)
    
    # Preprocessing steps
    df = df.drop(['Name', 'Ticket', 'Cabin', 'PassengerId'], axis=1, errors='ignore')
    df['Age'] = df['Age'].fillna(df['Age'].median())
    df = pd.get_dummies(df, columns=['Sex'], drop_first=True)
    
    # Save to SQLite
    df.to_sql(output_table, conn, if_exists='replace', index=False)
    conn.close()
    print(f"Preprocessed data saved to table '{output_table}' in database {db_path}")

with DAG(
    'data_preprocessing_pipeline',
    default_args=default_args,
    description='A simple data preprocessing pipeline',
    catchup=False,
) as dag:

    load_data = PythonOperator(
        task_id='load_csv_to_sqlite',
        python_callable=load_csv_to_sqlite,
        op_kwargs={
            'csv_path': '/opt/airflow/task_data/raw/titanic.csv',  # Update this path
            'db_path': '/opt/airflow/task_data/db/titanic.db',  # Update this path
            'table_name': 'raw_data'
        }
    )

    preprocess_data = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess,
        op_kwargs={
            'input_table': 'raw_data',
            'output_table': 'preprocessed_data',
            'db_path': '/opt/airflow/task_data/db/titanic.db'  # Should match the db_path above
        }
    )

    load_data >> preprocess_data