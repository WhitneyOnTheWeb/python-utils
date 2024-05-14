from airflow import DAG
from airflow.models import DagRun
from airflow.operators.empty import EmptyOperator
import include.helpers.dag_task_blocks as dtb


def generate_dag(dag_args, default_args, tasks, trigger_dags=None):
    """
    Generates a linear DAG using a list of tasks passed in as an argument.
    :param tasks:
    :param dag_args:
    :param default_args:
    :param trigger_dags:
    """

    generated_dag = DAG(
        dag_id=dag_args['dag_id'],
        description=dag_args['description'],
        schedule_interval=dag_args['schedule_cron'],
        start_date=dag_args['start_date'],
        catchup=dag_args['catchup'],
        tags=dag_args['tags'],
        concurrency=dag_args['concurrency'],
        max_active_runs=dag_args['max_active_runs'],
        default_args=default_args
    )

    with generated_dag:

        begin = EmptyOperator(task_id='begin')

        end = EmptyOperator(
            task_id='end',
            trigger_rule='all_success'
        )

        previous_task = begin

        for task in tasks:
            task.set_upstream(previous_task)
            previous_task = task
        task.set_downstream(end)

    return generated_dag
