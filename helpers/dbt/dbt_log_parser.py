import re
from io import StringIO


class DbtLogParser:
    def __init__(self, cmd_type, cmd_out):
        self.cmd_type = cmd_type
        self.dbt_logs = self._prep_logs(cmd_out)
        self.has_execution_error = self.check_for_dbt_execution_error()
        self.has_dbt_test = self.check_for_dbt_tests()

        self.start_line_idx = self.get_start_line_idx()
        self.end_line_idx = self.get_end_line_idx()
        self.metadata_idx = self.get_execution_metadata_idx()
        self.model_start_idx = self.get_model_start_idx()
        self.execution_details = self.get_execution_details()
        self.status_summary = self.get_status_summary()

        # helper properties for populating slack_alert for dbt task run
        if cmd_type == 'source freshness':
            self.model_end_idx = self.end_line_idx - 2
            self.execution_logs = self.get_freshness_full_logs()
            self.error_warn_logs = self.get_freshness_error_logs() + self.get_freshness_warn_logs()
            self.is_successful = True if len(self.get_freshness_error_logs()) == 0 else False
            self.has_warnings = True if len(self.get_freshness_warn_logs()) > 0 else False
            self.has_errors = True if len(self.get_freshness_error_logs()) > 0 else False
        elif cmd_type in ['run', 'test']:
            self.model_end_idx = self.get_model_end_idx()
            self.execution_logs = self.get_execution_logs()
            self.error_warn_logs = self.get_error_warn_logs()
            self.is_successful = self.check_for_success()
            self.has_warnings = self.check_for_warnings()
            self.has_errors = self.check_for_errors()

        if self.has_execution_error:
            self.is_successful = False
            self.has_errors = True

    @staticmethod
    def _prep_logs(cmd_out):
        """
        Prepares the logs for parsing
        :param cmd_out:
        :return:
        """
        io_logs = StringIO(cmd_out)
        logs = []
        for line in io_logs:
            logs.append(line)
        return logs

    def check_for_dbt_execution_error(self):
        """
        Check for dbt runtime error
        :return:
        """
        for i, line in enumerate(self.dbt_logs):
            # check for dbt runtime error
            if 'Encountered an error' in line or 'Error in model' in line or 'Compilation Error' in line:
                return True
        return False

    def check_for_dbt_tests(self):
        """
        Check if dbt test command has tests to execute

        """
        if self.cmd_type == 'test':
            for i, line in enumerate(self.dbt_logs):
                if 'Nothing to do' in line:
                    return False
            # dbt test command had a test to execute
            return True
        # not a dbt test command
        return False

    def get_start_line_idx(self):
        """
        Get the start line index of the dbt log line
        :return:
        """
        for i, line in enumerate(self.dbt_logs):
            if 'Running with dbt' in line:
                return i

    def get_end_line_idx(self):
        """
        Get the end line index of the dbt log line
        :return:
        """
        for i, line in enumerate(self.dbt_logs):
            if 'Done.' in line:
                return i
        # there wasn't a done line, so check for a finishing running line instead
        for i, line in enumerate(self.dbt_logs):
            if 'Finished running' in line:
                return i
        return len(self.dbt_logs) - 1

    def get_execution_metadata_idx(self):
        """

        :return:
        """
        if self.cmd_type in ['run', 'test']:
            for i, line in enumerate(self.dbt_logs):
                # return success confirmation index
                if 'Finished running' in line:
                    return i
                # return execution error index
                elif 'Encountered an error' in line or 'Error in model' in line or 'Compilation Error' in line:
                    return i
                elif self.cmd_type == 'test' and 'Nothing to do' in line:
                    # return no-tests-to-execute index
                    return i
        elif self.cmd_type == 'source freshness':
            return None

    def get_model_start_idx(self):
        """

        :return:
        """
        if not self.has_execution_error:
            for i, line in enumerate(self.dbt_logs):
                if 'Concurrency: ' in line:
                    return i + 2
        return self.metadata_idx

    def get_model_end_idx(self):
        """

        :return:
        """
        if not self.has_execution_error:
            return self.metadata_idx - 1
        return len(self.dbt_logs) - 1

    def get_freshness_full_logs(self):
        """

        :return:
        """
        logs = self.dbt_logs[self.model_start_idx:self.end_line_idx - 2]
        clean_logs = []
        for line in logs:
            clean_logs.append(self.remove_log_line_timestamp(line))
        return ''.join(clean_logs)

    def get_freshness_warn_logs(self):
        """

        :return:
        """
        logs = self.dbt_logs[self.model_start_idx:self.end_line_idx - 2]
        clean_logs = []
        for line in logs:
            clean_logs.append(self.remove_log_line_timestamp(line))
        logs = [line for line in clean_logs if 'WARN' in line]
        return ''.join(logs)

    def get_freshness_error_logs(self):
        """

        :return:
        """
        logs = self.dbt_logs[self.model_start_idx:self.end_line_idx - 2]
        clean_logs = []
        for line in logs:
            clean_logs.append(self.remove_log_line_timestamp(line))

        logs = [line for line in clean_logs if 'ERROR' in line]
        return ''.join(logs)

    def get_execution_logs(self):
        """

        :return:
        """
        if (self.has_execution_error
                or (self.cmd_type == 'test' and not self.has_dbt_test)):
            logs = self.dbt_logs[self.metadata_idx:]
        elif 'Completed with ' in self.dbt_logs[self.metadata_idx]:
            logs = self.dbt_logs[self.metadata_idx:-2]
        else:
            logs = self.dbt_logs[self.model_start_idx:self.model_end_idx]

        clean_logs = []
        for line in logs:
            clean_logs.append(self.remove_log_line_timestamp(line))
        return ''.join(clean_logs)

    def get_execution_details(self):
        """

        :return:
        """
        if self.has_execution_error:
            return self.dbt_logs[self.metadata_idx + 1]
        elif ((self.cmd_type == 'test' and not self.has_dbt_test)
                or self.metadata_idx == len(self.dbt_logs) - 1):
            return self.dbt_logs[self.metadata_idx]
        elif self.cmd_type in ['run', 'test']:
            return self.dbt_logs[self.metadata_idx + 2]
        return None

    def get_error_warn_logs(self):
        """

        :return:
        """
        if (self.has_execution_error
                or (self.cmd_type == 'test' and not self.has_dbt_test)):
            return self.get_execution_logs()

        logs = self.dbt_logs[self.metadata_idx + 4:self.end_line_idx - 1]
        clean_logs = []
        for line in logs:
            clean_logs.append(self.remove_log_line_timestamp(line))
        return ''.join(clean_logs)

    def check_for_success(self):
        """

        :return:
        """
        return True if 'Completed successfully' in self.get_execution_details() else False

    def check_for_errors(self):
        """

        :return:
        """
        status = self.get_execution_details()
        if re.search(r"Completed with \d+", status):
            n_errors = status.split('Completed with ')[1]
            n_errors = int(n_errors.split(' ')[0])
            return True if n_errors > 0 else False

    def check_for_warnings(self):
        """

        :return:
        """
        status = self.get_execution_details()
        if re.search(r"Completed with \d+", status):
            n_warns = status.split(' and ')[1]
            n_warns = int(n_warns.split(' ')[0])
            return True if n_warns > 0 else False

    def count_freshness_errors(self):
        return len(self.get_freshness_error_logs().split('\n')) - 1

    def count_freshness_warnings(self):
        return len(self.get_freshness_warn_logs().split('\n')) - 1

    def get_status_summary(self):
        """

        :return:
        """
        summary = {}
        if self.has_execution_error:
            summary = {
                'status': {
                    'error': 1,
                    'warn': 0
                },
                'meta': {
                    'execution_error': self.has_execution_error
                }
            }
        elif (self.cmd_type == 'run'
                or (self.cmd_type == 'test' and self.has_dbt_test)):
            status = self.dbt_logs[self.end_line_idx].replace('\n', '')
            summary['status'] = {}

            if '=' in status:
                status = status.split(' ')[3:]
                for s in status:
                    k, v = s.split('=')
                    summary['status'][k.lower()] = int(v)
            else:
                summary['status'] = {
                    'error': 0,
                    'warn': 0
                }

            summary['meta'] = {}
            meta = self.remove_log_line_timestamp(self.dbt_logs[self.metadata_idx].replace('\n', ''))
            if len(meta.split(', ')) == 3:
                run_time = meta.split(', ')[2]
            elif len(meta.split(', ')) == 2:
                run_time = meta.split(', ')[1]
            else:
                run_time = meta

            # remove the period that isn't needed for the runtimestamp
            run_time = run_time[:-1]

            summary['meta']['execution_error'] = self.has_execution_error
            if ' hook in ' in run_time:
                _, summary['meta']['execution_duration'] = run_time.split(' hook in ')
            elif ' model in ' in run_time:
                _, summary['meta']['execution_duration'] = run_time.split(' model in ')
            elif ' tests in ' in run_time:
                _, summary['meta']['execution_duration'] = run_time.split(' tests in ')
            else:
                summary['meta']['execution_duration'] = run_time
            return summary

        elif self.cmd_type == 'test' and not self.has_dbt_test:
            summary = {
                'status': {
                    'error': 0,
                    'warn': 0
                },
                'meta': {
                    'execution_error': self.has_execution_error
                }
            }
        else:
            summary = {
                'status': {
                    'error': self.count_freshness_errors(),
                    'warn': self.count_freshness_warnings()
                },
                'meta': {
                    'execution_error': self.has_execution_error
                }
            }
        return summary

    @staticmethod
    def remove_log_line_timestamp(line):
        """

        :param line:
        :return:
        """
        clean_line = re.sub(r"(\d+\:\d+\:\d+(\s+)?)", "", line)
        clean_line = re.sub(r"(\[\d+m)", "", clean_line)
        clean_line = clean_line.replace('', '')
        return clean_line
