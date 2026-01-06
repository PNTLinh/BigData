"""
XCom DAG - Sample DAG about sharing data between tasks with XCom
"""
from __future__ import annotations

import pendulum

from airflow.sdk import DAG, task

@dag(
    dag_id="xcom_example",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["xcom", "data-sharing"],
)
def xcom_dag():
    """
    ### XCom DAG
    Sample DAG about how to share data between tasks using XCom.
    """
    
    @task
    def extract_data():
        """### Extract Data
        Extract data and return to share with other tasks
        """
        data = {
            "users": [
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 25},
                {"id": 3, "name": "Charlie", "age": 35},
            ],
            "total": 3,
        }
        print(f"Extracted {data['total']} users")
        return data
    
    @task
    def process_data(data: dict):
        """### Process Data
        Receive data from extract_data and process
        """
        users = data["users"]
        total_age = sum(user["age"] for user in users)
        avg_age = total_age / len(users)
        
        result = {
            "total_users": len(users),
            "total_age": total_age,
            "average_age": avg_age,
        }
        print(f"Processed data: {result}")
        return result
    
    @task
    def save_summary(summary: dict):
        """### Save Summary
        Receive summary from process_data and save
        """
        print(f"Saving summary: {summary}")
        print(f"Average age: {summary['average_age']:.2f}")
        return f"Summary saved: {summary['total_users']} users processed"
    
    # Define dependencies with data passing
    extracted = extract_data()
    processed = process_data(extracted)
    save_summary(processed)

# Create DAG instance
xcom_dag()

