import pandas as pd
import os
import psycopg2
import logging as log
from include.helpers.utils import get_env_var, get_connection_info


def get_crdb_conn_params(svc: str, env: str):
    """

    :param svc:
    :param env:
    :return:
    """
    conn_id = f'crdb_{env}_{svc}'
    conn = get_connection_info(conn_id)

    params = {
        'host': conn.host,
        'port': conn.port,
        'user': conn.login,
        'password': conn.password
    }

    return params


def get_crdb_client(svc: str, env: str, database: str):
    """

    :param svc:
    :param env:
    :param database:
    :return:
    """
    log.info(f'Service: {svc}--{env}--{database}')
    params = get_crdb_conn_params(svc, env)

    host = params['host']
    password = params['password']

    log.info(f'Host reset to: {host}')

    crdb_client = CockroachDB(
        svc=svc,
        env=env,
        database=database,
        user=params['user'],
        password=password,
        host=host,
        port=params['port']
    )
    return crdb_client


class CockroachDB:
    def __init__(self, svc: str, env: str, database: str, user: str, password: str, host: str, port: str) -> None:
        """
        Instance of a psycopg2 object created with connection properties
        to a specific CockroachDB database.

        :param svc: abbreviation for service used in database name
        :param env: workspace in which to make changes to (dev, qa, prod)
        :param database: name of the CRDB database to connect to
        :param user: Username to use when connecting to CockroachDB
        :param password: Password for user
        :param host: Endpoint for the CockroachDB host
        :param port: Port to access CockroachDb host
        :return: None
        """
        self.service = svc
        self.env = env.lower()
        self.dbname = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None

    def connect(self) -> None:
        """
        Creates a connection object for querying CockroachDB with psycopg2,
        and assigns it to the self.conn parameter. Inherits credentials from
        the parent CockroachDB class instance.

        :return: None
        """
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.dbname,
            user=self.user,
            password=self.password,
            sslmode='require'
        )

    def close_connection(self) -> None:
        """
        Closes open connection CockroachDB.
        :return: None
        """
        try:
            self.conn.close()
        except Exception as e:
            log.info(f'Unable to close connection: {e}')

    def get_cursor(self) -> psycopg2:
        """
        Gets and returns a psycopg2.connection cursor for executing
        queries against CockroachDB.

        :return: cursor object for querying CockroachDB
        """
        return self.conn.cursor()

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Executes a query against CockroachDB, and returns the result set as a
        pandas DataFrame.

        :param query: query to run against CockroachDB database
        :return: pandas DataFrame with query results
        """
        self.connect()
        cur = self.get_cursor()
        cur.execute(query)

        try:
            columns = [d[0] for d in cur.description]
            res = pd.DataFrame(
                cur.fetchall(),
                columns=columns
            )
        except TypeError as e:
            res = None

        self.close_connection()
        return res

    def get_jobs(self, job_ids: list = None,
                 is_running: str = False) -> pd.DataFrame:
        """
        Gets information on jobs in CockroachDB.

        :param job_ids: Optional. list of specific job_id values to lookup
        :param is_running: Optional. flag to return only running jobs
        :return: DataFrame containing information on jobs in CockroachDB
        """
        # TODO: convert to str param that handles for status of 'running' or 'paused' changefeeds
        status = "status = 'running'"

        if job_ids is None:
            status = f' WHERE {status}' if is_running else ''
            query = f'SELECT * FROM [SHOW JOBS]{status};'
        else:
            status = f' AND {status}' if is_running else ''
            job_ids = [f"'{str(j)}'" for j in job_ids]
            job_ids = ', '.join(job_ids)
            query = f'SELECT * FROM [SHOW JOBS] WHERE job_id IN ({job_ids}){status};'

        df_jobs = self.execute_query(self, query)
        self.close_connection()

        return df_jobs

    def cancel_job(self, job_id: int):
        """
        Cancels a single active job running in CockroachDB.

        :param job_id: identifier for the running job to cancel

        Requires a commit so that the data manipulation is processed
        """
        # run cancel job operation for job_id

        query = f"CANCEL JOB '{job_id}';"
        log.info(f'{query}')
        self.connect()
        cur = self.get_cursor()
        cur.execute(query)
        desc = cur.description
        log.info(f'Description: {desc}')

        self.conn.commit()

        self.close_connection()

    def get_tables(self, db_schema: str, filter_str: str = None) -> pd.DataFrame:
        if filter_str is None:
            filter_str = ''

        query_tables = f"""
            select
                table_schema,
                table_name
            from information_schema.tables
            where table_schema = '{db_schema}'
            {filter_str};
        """

        df_tables = self.execute_query(query_tables)
        return df_tables

    def get_table_schema(self, db_schema: str, filter_str: str = None) -> pd.DataFrame:
        """
        Queries CockroachDB information_schema for a given table to get a list
        of columns it contains and their corresponding datatypes

        :param db_schema: Name of the table to get the schema for
        :return: pd.DataFrame
        """
        if filter_str is None:
            filter_str = ''

        # query table schema in crdb
        query_schema = f"""
        select distinct
            table_schema,
            table_name,
            column_name,
            data_type
        from information_schema.columns
        where table_schema = '{db_schema}'
        {filter_str};
        """
        df_schema = self.execute_query(query_schema)
        return df_schema

    def get_table_primary_keys(self, db_schema: str, filter_str: str = None) -> pd.DataFrame:
        """
        Queries CockroachDB information_schema for a given table to get a list
        of columns flagged with a PRIMARY KEY identity

        :param db_schema: Name of the table to get the primary keys for
        :param filter_str:
        :return: pd.DataFrame
        """
        if filter_str is None:
            filter_str = ''

        query_pks = f"""
        select distinct
            table_schema,
            table_name,
            column_name
        from information_schema.key_column_usage
        where table_schema = '{db_schema}'
        {filter_str}
        and (constraint_name = 'primary'
        or constraint_name like 'pk_%'
        or constraint_name like '%_pkey'
        or constraint_name like '%_pk');
        """
        df_pks = self.execute_query(query_pks)
        return df_pks

    def get_schema_table_dossier(self, db_schema: str, filter_str: str = None) -> pd.DataFrame:
        """

        """
        if filter_str is None:
            filter_str = ''

        query_tables = f"""
            with table_info as (
                select distinct
                    cols.table_schema,
                    cols.table_name,
                    cols.column_name,
                    cols.data_type,
                    case when pks.column_name is not null
                        then true else false
                    end as is_primary_key
                from information_schema.columns as cols
                    left join information_schema.key_column_usage as pks
                        on cols.table_schema = pks.table_schema
                        and cols.table_name = pks.table_name
                        and cols.column_name = pks.column_name
                        and (pks.constraint_name = 'primary'
                        or pks.constraint_name like 'pk_%'
                        or pks.constraint_name like '%_pkey'
                        or pks.constraint_name like '%_pk')
                where cols.table_schema = '{db_schema}'
            ),
            key_check as (
                select
                    table_schema,
                    table_name,
                    max(cast(is_primary_key as bool)) as has_primary_key
                from table_info
                group by 1, 2
            )

            select
                t.table_schema,
                t.table_name,
                t.column_name,
                t.data_type,
                case when k.has_primary_key = False and t.column_name = 'rowid'
                    then True else t.is_primary_key end as is_primary_key
            from key_check as k
                join table_info as t
                    on k.table_schema = t.table_schema
                    and k.table_name = t.table_name
            {filter_str};
        """

        df_tables = self.execute_query(query_tables)
        return df_tables
