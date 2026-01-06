"""
Branching DAG - DAG with conditional branching logic
"""
from __future__ import annotations

import pendulum

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.empty import EmptyOperator

@dag(
    dag_id="branching_example",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["branching", "example"],
)
def branching_dag():
    """
    ### Branching DAG
    Sample DAG with conditional branching to learn how to handle conditional logic.
    """
    
    start = EmptyOperator(task_id="start")
    
    @task.branch
    def check_condition(**context):
        """### Check Condition
        Branch based on condition (e.g., day of week)
        """
        # Get execution date from context
        execution_date = context["data_interval_start"]
        day_of_week = execution_date.weekday()
        
        # If Monday (0) or Friday (4), run both branches
        if day_of_week == 0 or day_of_week == 4:
            return ["branch_a", "branch_b"]
        # If weekend, only run branch_a
        elif day_of_week >= 5:
            return "branch_a"
        # Weekdays only run branch_b
        else:
            return "branch_b"
    
    branching = check_condition()
    
    branch_a = EmptyOperator(task_id="branch_a")
    branch_b = EmptyOperator(task_id="branch_b")
    
    # Join task - runs after both branches complete
    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",  # Run if at least 1 branch succeeds
    )
    
    end = EmptyOperator(task_id="end")
    
    # Define dependencies
    start >> branching
    branching >> branch_a >> join
    branching >> branch_b >> join
    join >> end

# Create DAG instance
branching_dag()

