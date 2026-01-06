"""
Hello World DAG - Simplest DAG to get familiar with Airflow
"""
from __future__ import annotations

import pendulum

from airflow.sdk import DAG, task

# Define DAG with @dag decorator
@dag(
    dag_id="hello_world",
    schedule=None,  # Manual trigger only
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["tutorial", "hello-world"],
)
def hello_world_dag():
    """
    ### Hello World DAG
    Simplest DAG with a single task.
    """
    
    @task
    def hello_task():
        """### Hello Task
        Simple task that prints "Hello World!"
        """
        print("Hello World!")
        print("Welcome to Apache Airflow!")
        return "Hello World completed successfully!"
    
    # Call task to add to DAG
    hello_task()

# Create DAG instance
hello_world_dag()

