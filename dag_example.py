from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import subprocess

# Função para executar um script Python
def executar_script(script_path):
    subprocess.run(["/Users/andrebezerra/Desktop/Dev/venv/bin/python", script_path], check=True)

# Configuração básica do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    'executar_scripts_duckdb',
    default_args=default_args,
    schedule_interval=None,  # Execução manual
) as dag:
    
    # Tarefa para executar script1.py
    task1 = PythonOperator(
        task_id='pokemon_data',
        python_callable=executar_script,
        op_args=['/Users/andrebezerra/Desktop/Dev/data_with_duckdb/pokemon_data.py']
    )
    
    # Tarefa para executar script2.py
    task2 = PythonOperator(
        task_id='types_data',
        python_callable=executar_script,
        op_args=['/Users/andrebezerra/Desktop/Dev/data_with_duckdb/types_data.py']
    )
    
    # Tarefa para executar script3.py
    task3 = PythonOperator(
        task_id='stats_data',
        python_callable=executar_script,
        op_args=['/Users/andrebezerra/Desktop/Dev/data_with_duckdb/stats_data.py']
    )

    # Definindo a ordem de execução
    task1, task2 >> task3