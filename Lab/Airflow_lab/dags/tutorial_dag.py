"""
Tutorial DAG - Sample DAG with multiple tasks and dependencies
"""
from __future__ import annotations

import pendulum

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator

@dag(
    dag_id="tutorial_dag",
    schedule="@daily",  # Run daily
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["tutorial", "example"],
)
def tutorial_dag():
    """
    ### Tutorial DAG
    Sample DAG with multiple tasks and dependencies to learn how to use Airflow.
    """
    
    # Task 1: Bash Operator
    start_task = BashOperator(
        task_id="start",
        bash_command="echo 'Starting pipeline...'",
    )
    
    # Task 2: Python Task with @task decorator
    @task
    def extract_data():
        """### Extract Data
        Simulate data extraction
        """
        print("Extracting data from source...")
        return {"status": "extracted", "records": 100}
    
    # Task 3: Transform data
    @task
    def transform_data(data: dict):
        """### Transform Data
        Transform extracted data
        """
        print(f"Transforming {data['records']} records...")
        return {"status": "transformed", "records": data["records"]}
    
    # Task 4: Load data
    @task
    def load_data(data: dict):
        """### Load Data
        Load transformed data to destination
        """
        print(f"Loading {data['records']} records to destination...")
        return {"status": "loaded", "records": data["records"]}
    
    # Task 5: Bash Operator
    end_task = BashOperator(
        task_id="end",
        bash_command="echo 'Pipeline completed!'",
    )
    
    # Define dependencies
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    loaded_data = load_data(transformed_data)
    
    start_task >> extracted_data >> transformed_data >> loaded_data >> end_task

# Create DAG instance
tutorial_dag()

