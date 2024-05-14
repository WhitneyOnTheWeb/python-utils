import base64
import datetime as dt
import os
import re
import sys
import json
import requests
import logging as log

from airflow.models import Variable
from google.cloud import bigquery as bq
from google.oauth2 import service_account as sa
from airflow.configuration import get_airflow_home as airflow_home
from airflow.hooks.base import BaseHook


def get_current_datetime_utc():
    """
    Returns current datetime in UTC timezone
    """
    return dt.datetime.now(dt.timezone.utc)


def read_json_file(file):
    with open(file, 'r') as f:
        return json.loads(f.read())


def write_json_file(filepath, jsont, format=False):
    if not isinstance(jsont, str):
        if format:
            jsont = json.dumps(jsont, indent=4)
        else:
            jsont = json.dumps(jsont)

    with open(filepath, 'w') as f:
        f.write(jsont)


def base64_encode_string(text: str) -> base64:
    """
    Take a raw text string, converts it to utf8 bytes, and base64 encodes it.

    :param text: string to base64 encode
    :return: base64 encoded bytes object
    """
    return base64.b64encode(bytes(text, encoding='utf8'))


def base64_decode_string(b_text: bytes) -> str:
    """
    Takes a base64 encoded utf-8 byres object, decodes it and returns the value
    in plain text
    :param b_text: base64 encoded bytes object
    :return: plain text string
    """
    return base64.b64decode(b_text).decode('utf8')


def get_connection_info(conn_id):
    conn = BaseHook.get_connection(conn_id)
    return conn


def get_env_var(env_var_name, required=True, deserialize_json=False):
    """
    A function which checks for an environment variable and quits the script if it does not exist;
    meant for Airflow, but generic enough to be used elsewhere.
    """
    try:
        var = Variable.get(env_var_name, deserialize_json=deserialize_json)
    except:
        var = os.environ.get(env_var_name)

    # Check if it exists, otherwise exit
    if required and not var:
        print('ERROR: Missing required environment variable {}'.format(env_var_name))
        sys.exit(-1)

    return var


def get_gcp_conn_credentials(keyfile_path):
    """
    Uses the google.oauth2.service_account library to
    create a basic connection object for GCP
    """
    credentials = sa.Credentials.from_service_account_file(
        keyfile_path,
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    return credentials


def get_random_words():
    word_site = 'https://www.mit.edu/~ecprice/wordlist.10000'
    response = requests.get(word_site)
    return response.content.splitlines()


def to_snake_case(key):
    """
    Takes a CamelCase key (string) and converts it to snake_case
    """
    key = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', key)
    key = re.sub('__([A-Z])', r'_\1', key)
    key = re.sub('([a-z0-9])([A-Z])', r'\1_\2', key)
    return key.lower()


def to_camel_case(key):
    """
    Why would you want to do this? :( Hopefully only used when
    trying to get something away from camelCase or when having to
    deal with some legacy camelCase
    """
    key = key.split('_')
    key = key[0] + ''.join(ele.title() for ele in key[1:])
    return key


def chunk_list(group, n=10):
    """
    Takes a list and converts it to a list of lists, where
    entries are grouped into a new list at every nth index.

    :param group: List of table names to group
    :param n: Optional. Size of table groups to make, defaults to 10.
    :return: List of chunked groups
    """
    # list comprehension to group tables on every nth item
    chunked_list = [group[i:i + n] for i in range(0, len(group), n)]
    return chunked_list


def dump_df_to_jsonl(df):
    """
    Transforms pandas DataFrame into JSONL string in the format

    {"key": "value"}
    {"key": "value2"}

    :param df:
    :return:
    """
    try:
        data = df.to_json(
            orient='records',
            lines=True
        )
        print(f'Saved DataFrame as JSONL string')
        return data
    except Exception as e:
        print(f'Unable to write DataFrame to JSON: {e}')
        raise e


def dump_df_to_json_list(df):
    """
        Transforms pandas DataFrame into JSON object list in the format

        [
            {"key": "value"},
            {"key": "value2"}
        ]

        :param df:
        :return:
        """
    try:
        data = df.to_json(
            orient='records'
        )
        print(f'Saved DataFrame as JSON object list')
        return data
    except Exception as e:
        print(f'Unable to write DataFrame to JSON: {e}')
        raise e


def ts_to_datetime(ts):
    """
    Take the Airflow ts parameter and format it into a datetime
    """
    log.info(f'Input timestamp: {ts}')
    if '.' in ts:
        head, sep, tail = ts.partition('.')
    if '.' not in ts:
        head, sep, tail = ts.partition('+')
    formatted_ts = dt.datetime.strptime(head, '%Y-%m-%dT%H:%M:%S')
    log.info(f'Formatted timestamp: {formatted_ts}')
    return formatted_ts


def read_bigquery_schema_from_file(filepath):
    result = []
    file_content = read_file(filepath)
    json_content = json.loads(file_content)
    for field in json_content:
        result.append(bq.SchemaField(
            name=field.get('name'),
            field_type=field.get('type', 'STRING'),
            mode=field.get('mode', 'NULLABLE'),
            description=field.get('description')))
    return result


def read_file(filepath):
    with open(filepath) as file_handle:
        content = file_handle.read()
        return content


def get_airflow_home():
    return airflow_home()


def get_astro_home():
    return '/home/astro'


def get_input_params(input_params_keys, **kwargs):
    """
        Method to grabbing input parameters from the DAG ConfigExpected behavior is there is an expected params list
        passed in from the runner and we use that as keys to grab from the config. If something is missing log that as
        an error and put None in its place. So in the event of optional params we dont need to pass them in and default
        them to None

        params:
            input_params_keys:
                List of known param keys we want out of DAG config

        returns:
            input_param_values:
                List of param values taken from DAG config using keys passed in
    """
    input_param_values = []
    missing = []

    # get each required parameter value from the dag_run.conf
    for input_param_key in input_params_keys:
        try:
            param = kwargs['dag_run'].conf[input_param_key]
        except Exception as e:
            param = None
            missing.append(input_param_key)

        input_param_values.append(param)

    # raise error if any required parameters are missing
    if len(missing) > 0:
        log.error(f'KeyError: Please input value(s) for missing parameter(s): {", ".join(missing)}')

    return input_param_values


def read_script_from_file(path):
    log.info(f"Reading from path '{path}'")
    f = open(path, 'r')
    script = f.read()
    f.close()

    return script
