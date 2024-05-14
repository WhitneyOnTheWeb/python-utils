from include.configs.environment import DBT_ARTIFACTS_DIR
from include.helpers.dbt_log_parser import DbtLogParser
from include.helpers.utils import get_env_var
import subprocess as bash
import logging as log
import json


class DbtClient:
    def __init__(self, env='dev', target_path='target'):
        self.env = env

        # get dbt nodes and sources from the manifest file
        manifest = self.load_manifest(target_path)
        self.nodes = manifest['nodes']
        self.sources = manifest['sources']

        # assign keys from self.nodes to table models or tests
        self.table_models = None
        self.table_tests = None
        self.parse_node_objects()
        log.info('Successfully loaded DbtClient!')

    @staticmethod
    def load_manifest(target_path='target'):
        """
        loads the dbt manifest file from the target directory into a json object
        :param target_path:
        :return:
        """
        local_filepath = f"{DBT_ARTIFACTS_DIR}/manifest.json"
        with open(local_filepath) as f:
            data = json.load(f)

        if len(data.keys()) == 0:
            log.error('dbt manifest file is empty or missing!')
        else:
            log.info('dbt manifest file loaded successfully!')
        return data

    def parse_node_objects(self):
        """
        Takes the two kinds of dbt structures found within the nodes
        of the manifest, and parses them out into model and test
        properties that can be used in a more organized method
        """
        models = {}
        tests = {}

        for i in self.nodes.keys():
            node_type = i.split(".")[0]
            if node_type == 'model':
                models[i] = self.nodes[i]
            elif node_type == 'test':
                tests[i] = self.nodes[i]

        self.table_models = models
        self.table_tests = tests

    def melt_dbt_config(self, param: str, *fields):
        """
        Takes a type of object (key) found in a subcomponent of the manifest
        file for dbt, and unburies requested values of interest from the nested
        structure

        Supported param values: sources, nodes, table_models, table_tests

        Supported values for fields are keys nested within the corresponding
        manifest component
        """
        resp = {}
        class_obj = getattr(self, param)

        for i in class_obj.keys():
            if i not in resp.keys():
                resp[i] = {}
            for f in fields:
                resp[i][f] = class_obj[i][f]
        return resp

    # ---- dbt Metadata Getters -----------------------------------------------
    # TODO: add another helper function to deduplicate code for these getters
    def get_dbt_source_loaders(self):
        """
        Gets a list of different manually flagged loaders, with a list of raw
        source tables in the database each loader is associated with
        """
        fields = ['loader', 'relation_name']
        melt = self.melt_dbt_config('sources', None, None, fields)

        loaders = {}

        for source in melt.keys():
            loader = melt[source]['loader']
            ref = melt[source]['relation_name'].replace('`', '')

            if loader not in loaders.keys():
                loaders[loader] = [ref]
            else:
                loaders[loader].append(ref)
        return loaders

    def get_dbt_sources(self):
        """
        Gets a list of raw sources defined in dbt, with an accompanying list of
        unique_ids for any table_models referencing those raw sources
        """
        fields = ['source_name', 'unique_id']
        melt = self.melt_dbt_config('sources', None, None, fields)
        sources = {}

        for source in melt.keys():
            src_name = melt[source]['source_name']
            src_id = melt[source]['unique_id']

            # build a dictionary of sources in dbt manifest, and tables available within
            if src_name not in sources.keys():
                sources[src_name] = [src_id]
            else:
                sources[src_name].append(src_id)
        return sources

    def get_dbt_datasets(self):
        fields = ['schema', 'unique_id']
        melt = self.melt_dbt_config('nodes', 'model', None, fields)
        datasets = {}

        for node in melt.keys():
            schema = node['schema']
            table_model = node['unique_id']

            # build a dict of datasets and their tables populated in dbt manifest
            if schema not in datasets.keys():
                datasets[schema] = [table_model]
            else:
                datasets[schema].append(table_model)

        return datasets

    # ---- Source Getters -----------------------------------------------------
    def get_source_fqn(self, unique_id):
        return self.sources[unique_id]['fqn']

    def get_source_path(self, unique_id):
        fqn = self.get_source_fqn(unique_id)
        path = '.'.join(fqn[:-1])
        return path

    def get_source_dataset(self, unique_id):
        return self.sources[unique_id]['schema']

    def get_source_table_name(self, unique_id):
        return self.sources[unique_id]['identifier']

    def get_source_name(self, unique_id):
        return self.sources[unique_id]['source_name']

    def get_source_loaded_at_field(self, unique_id):
        return self.sources[unique_id]['loaded_at_field']

    def get_source_freshness_config(self, unique_id):
        return self.sources[unique_id]['freshness']

    def get_source_tags(self, unique_id):
        return self.sources[unique_id]['tags']

    # ---- Table Model Getters ------------------------------------------------
    def get_table_model_unique_id_by_name(self, name):
        for uid in self.table_models.keys():
            if self.get_table_model_name(uid) == name:
                return uid
        return f'{name} was not associated with a unique_id in the dbt model'

    def get_table_model_dependencies(self, unique_id):
        deps = self.table_models[unique_id]['depends_on']['nodes']
        deps = list(set([d for d in deps if d.startswith('model')]))
        return deps

    def get_table_model_fqn(self, unique_id):
        return self.table_models[unique_id]['fqn']

    def get_table_model_stub_path(self, unique_id):
        fqn = self.get_table_model_fqn(unique_id)
        path = '.'.join(fqn[:-1])
        return path

    def get_table_model_file_path(self, unique_id):
        return self.table_models[unique_id]['path']

    def get_table_model_dataset(self, unique_id):
        return self.table_models[unique_id]['schema']

    def get_table_model_name(self, unique_id):
        return self.table_models[unique_id]['name']

    def get_table_model_tags(self, unique_id):
        return self.table_models[unique_id]['tags']

    def get_table_model_sources(self, unique_id):
        return self.table_models[unique_id]['sources']

    def get_table_model_config(self, unique_id):
        return self.table_models[unique_id]['config']

    def get_table_model_materialized(self, unique_id):
        config = self.get_table_model_config(unique_id)
        return config['materialized']

    def get_table_model_raw_sql(self, unique_id):
        return self.table_models[unique_id]['raw_sql']

    def get_table_model_compiled_sql(self, unique_id):
        for key in self.table_models[unique_id].keys():
            log.info(f'{unique_id} | {key}')
        if 'compiled' in self.table_models[unique_id].keys():
            if 'compiled_sql' in self.table_models[unique_id].keys():
                return self.table_models[unique_id]['compiled_sql']
            elif 'compiled_code' in self.table_models[unique_id].keys():
                return self.table_models[unique_id]['compiled_code']
            return f'Compiled SQL for {unique_id} not found! Check manifest keys!'
        else:
            return f'No compiled SQL for {unique_id} found!'

    # ---- Data Model Getters -------------------------------------------------
    # TODO: add another helper function to help reduce code for data model getters
    def get_data_models_by_tag(self):
        tag_models = {}

        for uid in self.table_models.keys():
            tags = self.get_table_model_tags(uid)

            # pivot the data to make a list of unique_ids by tag
            for tag in tags:
                if tag not in tag_models.keys():
                    tag_models[tag] = [uid]
                elif uid not in tag_models[tag]:
                    tag_models[tag].append(uid)
        return tag_models

    # TODO: add another helper function to help reduce code for data model getters
    def get_data_models_by_model_stub(self, model):
        models = {}

        for uid in self.table_models.keys():
            if self.get_table_model_stub_path(uid).startswith(model):
                if model not in models.keys():
                    models[model] = [uid]
                elif uid not in models[model]:
                    models[model].append(uid)

        return models

    def get_data_models_by_path(self):
        path_models = {}

        for uid in self.table_models.keys():
            path = self.get_table_model_file_path(uid)
            path = path.split('/')
            path = '/'.join(path[:-1])

            # pivot the data to make a list of unique_ids by path
            if path not in path_models.keys():
                path_models[path] = [uid]
            elif uid not in path_models[path]:
                path_models[path].append(uid)
        return path_models

    def get_data_models_by_materialized(self):
        materialized_models = {}

        for uid in self.table_models.keys():
            mat = self.get_table_model_materialized(uid)

            # pivot the data to make a list of unique_ids by materialized type
            if mat not in materialized_models.keys():
                materialized_models[mat] = [uid]
            elif uid not in materialized_models[mat]:
                materialized_models[mat].append(uid)
        return materialized_models

    # ---- dbt Command Runners -------------------------------------------------
    @staticmethod
    def build_dbt_command(cmd_type, target, model_name=None, defer_dir=DBT_ARTIFACTS_DIR, full_refresh=False, vars={}):
        """
        Builds a dbt command for run, test, or source freshness

        :param cmd_type: run, test, source
        :param model_name: name of the model to run or test
        :param target: target profile to run dbt against
        :param defer_dir: directory to store dbt state files
        :param full_refresh: boolean to force a full refresh of the model
        :vars: dictionary of variables to pass to dbt
        :return:
        """
        log.info(f'Building `dbt {cmd_type}` command for {model_name}...')

        cmd = (
            f"dbt --no-write-json {cmd_type} --target {target}" +
            f" --profiles-dir {get_env_var('DBT_PROFILES_DIR')}"
        )

        # TODO: Rework all logics related to vars (dbt.py, dbt_dags.py, airflow_dbt.py)
        if cmd_type in ['run', 'test']:
            cmd += (
                f" --vars '{str(vars)}' " +
                f" --models {model_name}" +
                f" --defer --state {defer_dir}"
            )
        if cmd_type == 'run' and full_refresh:
            cmd += " --full-refresh"

        log.info(cmd)
        return cmd

    def execute_dbt_command(self, cmd_type, cmd):
        """
        Executes a dbt command of run, test, or source freshness via bash
        :param cmd_type:
        :param cmd:
        :return:
        """
        # TODO: type check for more supported dbt commands
        log.info(f'Executing `dbt {cmd_type}` command...')
        status, output = bash.getstatusoutput(cmd)
        log.info(f'`dbt {cmd_type}` Output: \n{output}')

        parser = DbtLogParser(cmd_type, output)

        # check execution status in dbt logs
        if parser.is_successful and not parser.has_warnings:
            # success
            log.info(f'`dbt {cmd_type}` Completed Successfully!')
            status = 'success'

        elif parser.has_errors:
            # error || (error + warning)
            log.error(f'`dbt {cmd_type}` Encountered Error(s)!')
            status = 'error'

        elif parser.has_warnings:
            # warning
            log.warning(f'`dbt {cmd_type}` Completed with Warning(s)!')
            status = 'warning'

        else:
            status = 'unknown'

        return status, output
