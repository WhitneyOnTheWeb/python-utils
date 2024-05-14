from airflow import DAG
from airflow.models import DagRun
from airflow.utils.db import provide_session
from airflow.models.dag import get_last_dagrun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import ShortCircuitOperator
import datetime as dt
import logging as log


def _get_last_execution_datetime_of(dag_id, **kwargs):
    """
    Returns the execution date of the most recent DAG run for the given dag_id
    """
    dag_runs = DagRun.find(dag_id=dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        log.info(dag_runs)
        log.info(f'Last execution date of {dag_id}: {dag_runs[0].execution_date}')
        return dag_runs[0].execution_date
    else:
        log.info(f'No previous DAG runs found for {dag_id}')
        return None


def _get_last_execution_datetime_with_session(dag_id, **kwargs):
    """
    Returns the execution date of the most recent DAG run for the given dag_id
    """
    @provide_session
    def _get_last_execution_date(exec_date, session=None, **kwargs):
        dag_last_run = get_last_dagrun(dag_id, session)
        return dag_last_run.execution_date
    return _get_last_execution_date


def check_short_circuit_interval_hour(dag_id, schedule_interval, **kwargs):
    """
    Checks if the interval between the most recent task run and the current time is greater than
    the task build interval, in hours
    """
    if schedule_interval is False:
        # no interval tag found, so run the dbt model task
        log.info(f'No schedule interval tag found, running dbt model as scheduled...')
        return True
    else:
        prev_dag_run = _get_last_execution_datetime_of(dag_id)
        log.info(f'Previous DAG run: {prev_dag_run}')
        log.info(f'Current time: {dt.datetime.now(dt.timezone.utc)}')
        hours_since_last_run = (dt.datetime.now(dt.timezone.utc) - prev_dag_run).seconds // 3600
        log.info(f'Hours since last DAG run: {hours_since_last_run}')
        if hours_since_last_run >= schedule_interval:
            # time has surpassed short-circuit interval, so run the dbt model task
            log.info('Short-circuit interval has been reached, running dbt model...')
            return True
        else:
            # time hasn't reached short-circuit interval, so don't run the dbt model task
            log.info('Short-circuit interval has not been reached, skipping dbt model...')
            return False


def check_model_tags_for_schedule_interval(tags, **kwargs):
    """
    Checks if a model has a schedule interval tag, and returns the interval in hours if it does
    """
    interval_tag = [i for i in tags if i.startswith('hour_interval')]
    if len(interval_tag) > 0:
        hour_interval = int(interval_tag[0].split(':')[1])
        return hour_interval
    else:
        return False


def add_trigger_dag_task(env, dag, model_group, dbt_task=False):
    """

    :param env:
    :param dag:
    :param model_group:
    """
    if dbt_task:
        task_name = f"trigger_dag__run_dbt__{model_group}__{env}"
        dag_name = f'run_dbt__{model_group}__{env}'
    else:
        task_name = f"trigger_dag__{model_group}"
        dag_name = model_group

    trigger_task = TriggerDagRunOperator(
        task_id=task_name,
        trigger_dag_id=dag_name,
        trigger_rule='none_failed',
        dag=dag
    )

    return trigger_task


def add_external_dag_sensor_task(env, dag, wait_dag, dbt_task=False, **kwargs):
    """
    :param env:
    :param dag:
    :param wait_dag:
    :param dbt_task:
    """
    if dbt_task:
        task_name = f"wait_on_dag__run_dbt__{wait_dag}__{env}"
        parent_dag_id = f'run_dbt__{wait_dag}__{env}'
    else:
        task_name = f"wait_on_dag__{wait_dag}"
        parent_dag_id = wait_dag

    sensor_task = ExternalTaskSensor(
        task_id=task_name,
        external_dag_id=parent_dag_id,
        external_task_id='end',
        allowed_states=["success"],
        mode="reschedule",
        execution_date_fn=_get_last_execution_datetime_with_session(parent_dag_id, **kwargs),
        poke_interval=dt.timedelta(minutes=5),
        timeout=dt.timedelta(hours=4),
        soft_fail=True,
        dag=dag
    )
    return sensor_task


def add_short_circuit_schedule_task(dag_id, hour_interval, model_name=None, task_group=None):
    """
    :param dag_id: name of the dag check prior runtime for against short-circuit window for model
    :param hour_interval: hours between task runs as an integer, regardless of if dag runs
    :param model_name: name of the dbt model to short circuit if not on the hour interval
    :param task_group: task group to add the short circuit task to
    """
    print(f"Short-Circuit Config: {dag_id} - {hour_interval} - {model_name}")
    task_name = f"short_circuit_check"
    if hour_interval is not False:
        task_name = task_name + f'__{hour_interval}'
    if model_name is not None:
        task_name = task_name + f'__{model_name}'

    short_circuit_task = ShortCircuitOperator(
        task_id=task_name,
        python_callable=check_short_circuit_interval_hour,
        ignore_downstream_trigger_rules=False,
        op_kwargs={
            'dag_id': dag_id,
            'schedule_interval': hour_interval,
            'task_name': 'short_circuit_check',
            'model_name': model_name
        },
        task_group=task_group
    )

    return short_circuit_task
