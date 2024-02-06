from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import csv


def get_dag_info_to_csv():
    dags = []
    for dag_id, dag in airflow.dag_bag.dags.items():
        schedule = dag.schedule_interval
        latest_execution = dag.latest_execution_date
        dags.append([dag_id, str(schedule), str(latest_execution)])

    filename = "/dags/environment_operations.csv"
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["DAG ID", "Schedule Interval", "Latest Execution"])
        writer.writerows(dags)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 26),
    # Add other necessary default arguments
}

with DAG(
    "dag_operations_maintenance",
    default_args=default_args,
    schedule_interval=None,  # Set the schedule_interval to None for one-time execution
    catchup=False,
) as dag:

    export_dags_to_csv = PythonOperator(
        task_id="export_dags_to_csv", python_callable=get_dag_info_to_csv
    )

    export_dags_to_csv
