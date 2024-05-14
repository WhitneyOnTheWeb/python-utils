import io
from google.cloud.storage import client as storage
from tempfile import NamedTemporaryFile
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from include.helpers.utils import get_gcp_conn_credentials, get_env_var
from include.configs.environment import DEFAULT_KEYFILE_PATH


def get_cloud_storage_client() -> storage.Client:
    """
    Creates an instance of the google.cloud.storage class
    using credentials defined with the custom helper function
    include.helpers.utils.get_gcp_conn_credentials()
    :return: Authenticated Google Cloud Storage client
    """
    credentials = get_gcp_conn_credentials(DEFAULT_KEYFILE_PATH)
    client = storage.Client(
        credentials=credentials,
        project=credentials.project_id
    )
    return client


def get_cloud_storage_client_via_hook(gcp_conn_id='google_cloud_default'):
    """

    :param gcp_conn_id:
    :return:
    """
    client = GCSHook(gcp_conn_id=gcp_conn_id)
    return client


def get_base64_encoded_gcp_credentials() -> dict:
    """

    :return: base64 encoded string for GCP Authentication
    """
    return get_env_var('GCP_KEYFILE_BASE64')


def get_connection_url(bucket: str, path: str) -> str:
    """

    :param bucket:
    :return:
    """
    creds = get_base64_encoded_gcp_credentials()
    url = (f"gs://{bucket}/{path}?" +
           f"&partition_format=hourly&file_size=32MB"
           f"&AUTH=specified&CREDENTIALS={creds}")
    return url


class GoogleCloudStorage:
    def __init__(self, gcp_project_id):
        self.client = get_cloud_storage_client()
        self.project_id = gcp_project_id

    def list_gcs_bucket_contents(self, bucket, prefix=None):
        """
        Returns path and file list for a given GCS bucket
        """
        f_list = self.client.list_blobs(bucket, prefix=prefix)
        return f_list

    def list_dirs_in_gcs_bucket_path(self, bucket, path):
        # only check for prefixes containing a slash, and ignore file blobs beyond 1 we have to load
        blobs = self.client.list_blobs(
            bucket,
            prefix=f'{path}/',
            delimiter='/',
            max_results=1
        )
        # force GCS to load blob data for the bucket
        next(blobs, ...)
        # extract dirs from blob prefixes
        dirs = list(blobs.prefixes)
        return dirs

    def download_file_from_gcs(self, bucket, path, file, file_dest):
        """
        Retrieves contents from bucket/path/file, and downloads
        them to local directory destination
        """
        bucket = self.client.get_bucket(bucket)
        blob = bucket.blob(f'{path}/{file}')
        blob.download_to_filename(file_dest)

    def open_from_gcs(self, bucket, filepath):
        """
        Retrieves contents from bucket/path/file, and prepares
        for use within functions
        """
        bucket = self.client.get_bucket(bucket)
        if bucket.blob(filepath).exists(self.client):
            blob = bucket.blob(filepath)
            blob_bytes = io.BytesIO(blob.download_as_string())
        else:
            blob_bytes = None
        return blob_bytes

    def write_json_string_to_gcs(self, dest_bucket, dest_path, dest_filename, data):
        """
        Take a string parameter and writes it as a file to GCS

        Note: originally designed as an aid for doing this with JSON files,
        but it works in general with string data, so the method name and
        references should likely be updated to reflect that
        """
        # must be in GCS VPC to work w/o authentication
        # need auth for local, should work w/o auth on Composer Airflow
        bucket = self.client.get_bucket(dest_bucket)
        blob = bucket.blob(f'{dest_path}/{dest_filename}')
        blob.upload_from_string(data)

        print(f'File uploaded to gs://{dest_bucket}/{dest_path}/{dest_filename}.')

    def write_file_to_gcs(self, dest_bucket, dest_path, dest_filename, filepath):
        """
        Retrieves a file from a stringified file path on the local system, and
        uploads it to a destination bucket/path/file on GCS
        """
        # must be in GCS VPC to work w/o authentication
        # need auth for local, should work w/o auth on Composer Airflow
        bucket = self.client.get_bucket(dest_bucket)
        blob = bucket.blob(f'{dest_path}/{dest_filename}')
        blob.upload_from_filename(filepath)

        print(f'File {filepath} uploaded to gs://{dest_bucket}/{dest_path}/{dest_filename}.')

    def write_data_as_file_to_gcs(self, dest_bucket, dest_path, dest_filename, data):
        """
        Take a non-string parameter and writes it as a file to GCS
        """
        bucket = self.client.get_bucket(dest_bucket)
        blob = bucket.blob(f'{dest_path}/{dest_filename}')

        with NamedTemporaryFile(mode='w+b') as temp:
            temp.write(data)
            with open(temp.name, 'rb') as pdf:
                blob.upload_from_file(pdf, rewind=True)
            temp.close()

