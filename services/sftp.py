import paramiko
import datetime as dt
import logging as logger


def get_sftp_client():
    """
    Gets a basic unauthenticated SSH client to use for SFTP
    file transfers
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    return ssh


class SFTPClient:
    def __init__(self, host, user, password, cwd):
        self.host = host
        self.user = user
        self.password = password
        self.cwd = cwd
        self.client = get_sftp_client()

    def connect_to_sftp_host(self):
        self.client.connect(
            self.host,
            username=self.user,
            password=self.password
        )
        ftp_client = self.client.open_sftp()
        logger.info('SFTP client connection opened...')
        return ftp_client

    def get_sftp_directory_structure(self):
        """
        Will return both directories and files in the cwd (root) of the sftp host.
        Designed during JPM implementation, which only deals in files, likely needs review
        to work in a wider application where directory traversal is required
        """
        with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.banner_timeout = 200
            ssh.timeout = 200
            ssh.auth_timeout = 200
            logger.info('Opening connection to SFTP Host...')
            ssh.connect(
                self.host,
                username=self.user,
                password=self.password
            )
            ftp = ssh.open_sftp()
            logger.info('SFTP client connection opened...')

            logger.info('Getting top level file structure...')
            dir_struct = ftp.listdir()
            ftp.close()

        return dir_struct

    def get_files_by_name(self, files, dl_dir):
        """
        Opens a live connection to the SFTP host, and returns a
        list of files by name contained therein
        """
        try:
            logger.info('Opening connection to SFTP Host...')
            ftp = self.connect_to_sftp_host()
        except Exception as e:
            logger.error('Problem establishing connection to SFTP Host...')
            raise e

        try:
            file_paths = []
            logger.info('Getting list of files from SFTP Host...')
            for file in files:
                fdir = f'{self.cwd}/{dl_dir}/{file}'
                ftp.get(file, fdir)
                filestats = ftp.stat(file)
                logger.info(f'{file} most recently accessed at {dt.datetime.fromtimestamp(filestats.st_atime)}')
                logger.info(f'{file} most recently modified at {dt.datetime.fromtimestamp(filestats.st_mtime)}')
                file_paths.append(fdir)
            return file_paths
        except Exception as e:
            logger.error(f'Error: {e}')
            raise e

