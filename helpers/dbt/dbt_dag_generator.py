from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.time_delta import TimeDeltaSensor
import datetime as dt
from include.runners.dbt import generate_dbt_airflow_tasks_for_path
import include.helpers.dag_task_blocks as dtb
from airflow.models import DagRun


def generate_dbt_dag(env, dag_args, dbt_args, default_args, trigger_dags=None,
                     wait_on_dags=None, wait_time_sec=None):
    """

    :param env:
    :param dag_args:
    :param dbt_args:
    :param default_args:
    :param trigger_dags:
    :param wait_on_dags:
    """
    model_group = dbt_args['model_group']
    dag_type = dbt_args['model_type']

    with DAG(
        dag_id=dag_args['dag_id'],
        description=dag_args['description'],
        schedule_interval=dag_args['schedule_cron'],
        start_date=dag_args['start_date'],
        catchup=dag_args['catchup'],
        tags=dag_args['tags'],
        concurrency=dag_args['concurrency'],
        max_active_runs=dag_args['max_active_runs'],
        default_args=default_args
    ) as dag:

        begin = EmptyOperator(task_id='begin')

        if wait_on_dags is not None:
            wait_on_dag_task = []
            for i, wait_dag in enumerate(wait_on_dags):
                wait_on_dag_task.append(dtb.add_external_dag_sensor_task(
                    env, dag, wait_dag, True
                ))
                wait_on_dag_task[i] >> begin

        end = EmptyOperator(
            task_id='end',
            trigger_rule='none_failed'
        )

        if wait_time_sec is not None:
            wait = TimeDeltaSensor(task_id="wait",
                                   delta=dt.timedelta(seconds=wait_time_sec),
                                   mode='reschedule')

        group_id = model_group.replace("/", "_") if '/' in model_group else model_group
        with TaskGroup(group_id=f'{group_id}__{env}') as task_gen:
            targs = {
                'model': model_group,
                'cmds': dbt_args['dbt_cmds'],
                'group': dbt_args['group_tasks_by_model_path'],
                'exclude': dbt_args['exclude_tags']
            }

            # dag generator will only support path-based DAGs in this current form
            __, tasks = generate_dbt_airflow_tasks_for_path(
                dag, env, targs['model'], targs['cmds'], targs['exclude'], targs['group'], env
            )

            for uid in tasks.keys():
                if len(tasks[uid]['dependencies']) > 0:
                    for dep in tasks[uid]['dependencies']:
                        if dep in tasks.keys():
                            if wait_time_sec is not None:
                                tasks[dep]['task'] >> wait >> tasks[uid]['short-circuit-task'] >> tasks[uid]['task']
                            else:
                                tasks[dep]['task'] >> tasks[uid]['short-circuit-task'] >> tasks[uid]['task']
                else:
                    tasks[uid]['short-circuit-task'] >> tasks[uid]['task']

        # add a trigger operator to kickoff downstream DAGs to run after this DAG completes,
        #   omitted for recommendations dev environment, since downstream DAG does not apply
        if env == 'dev':
            # add a trigger operator to kickoff qa DAGs to run after dev version
            task_gen >> dtb.add_trigger_dag_task('qa', dag, group_id, True) >> end

        if trigger_dags is not None and model_group != 'recommendations':
            for model_dag in trigger_dags:
                task_gen >> dtb.add_trigger_dag_task(env, dag, model_dag, True) >> end
        elif model_group == 'recommendations' and env != 'dev':
            for model_dag in trigger_dags:
                task_gen >> dtb.add_trigger_dag_task(env, dag, model_dag, False) >> end
        else:
            task_gen >> end

        begin >> task_gen

    return dag
