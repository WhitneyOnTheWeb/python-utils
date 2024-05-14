import json
import requests


from snowplow_tracker import Tracker, Emitter, SelfDescribingJson
from snowplow_snowball_factory import SnowplowSnowballFactory


import logging as log
# mainly used to enable logging output in Jupyter Notebooks
# logger = log.getLogger('snowplow_tracker.emitters')
# logger.setLevel(log.ERROR)


def get_bearer_token(api_key, url_stub):
    """
    Requires an API key to request a bearer token
    :return: Bearer token string for Snowplow APIs
    """
    url = f'{url_stub}/credentials/v2/token'
    headers = {
        'accept': 'application/json',
        'X-API-Key': api_key
    }
    resp = requests.get(url, headers=headers).json()
    token = resp['accessToken']
    bearer_token = f'Bearer {token}'
    return bearer_token


class SnowplowSnowballFight:
    def __init__(self, collector: str, api_key: str, org_id: str, namespace: str) -> None:
        self.org_id = org_id
        self.collector = collector

        # run GET request to Snowplow for a bearer token to use for interfacing with services
        self.api_url_stub = f'https://console.snowplowanalytics.com/api/msc/v1/organizations/{self.org_id}'
        self.bearer_token = get_bearer_token(api_key, self.api_url_stub)

        self.namespace = namespace
        self.app_id = 'snowball_fight'

        self.event_schemas = self.get_event_schemas()

        self.success = []
        self.fail = []

        #   ----this output was designed to print at runtime to demonstrate a tracker initialization
        print('-------- BEGIN SNOWPLOW APPLICATION SETUP -------')
        print('\n-- ----- EMITTER SETUP -----')
        print(f'-- Initializing Snowplow Event Emitter to {collector}...')
        # create an emitter that will gather event payloads being tracked by the application
        #   and forward them along to the collector
        self.emitter = Emitter(
            self.collector,
            on_success=self.successful,
            on_failure=self.failure
        )
        print(f'-- Snowplow Event Emitter successfully initialized!')

        print('\n-- ----- TRACKER SETUP -----')
        print(f'-- Initializing Snowplow Tracker with namespace={self.namespace}, app_id={self.app_id}, and Emitter...')
        self.tracker = Tracker(
            self.emitter,
            namespace=self.namespace,
            app_id=self.app_id
        )
        print(f'-- Snowplow Event Tracker successfully initialized!')
        # print('-- Successful and failed events can be checked in Kibana:')
        print('\n-------- END SNOWPLOW APPLICATION SETUP -------')

    def get_event_schemas(self):
        """
        Queries the Iglu Schema Registry via Data Structures API
        :return: JSON payload containing registered schema information
        """
        url = f'{self.api_url_stub}/data-structures/v1'
        headers = {
            'accept': 'application/json',
            'Authorization': self.bearer_token
        }
        resp = requests.get(url, headers=headers)

        try:
            return resp.json()
        except Exception as e:
            return resp.text

    # ------ Event Response Aggregators
    # Since v0.9.0, the on_success callback receives the array of successfully sent events
    def successful(self, arr):
        for event_dict in arr:
            self.success.append(event_dict)

    def failure(self, num, arr):
        print(str(num) + " events sent successfully!")
        for event_dict in arr:
            self.fail.append(event_dict)

    # ------ GET Helpers for Snowplow APIs
    def get_event_vendors(self):
        """
        Get a list of unique vendors with registered events
        """
        vendors = []
        for schema in self.event_schemas:
            if schema['vendor'] not in vendors:
                vendors.append(schema['vendor'])
        return sorted(vendors)

    def get_schemas_for_vendor(self, vendor):
        """
        Get a list of all registered schema information for a specific vendor
        :param vendor:
        :return:
        """
        schemas = []
        for schema in self.event_schemas:
            if schema['vendor'] == vendor:
                schemas.append(schema)
        return schemas

    def get_events_for_vendor(self, vendor):
        """
        Get a list of unique event names registered by a vendor
        :param vendor:
        :return:
        """
        events = []
        for schema in self.event_schemas:
            if schema['vendor'] == vendor and schema['name'] not in events:
                events.append(schema['name'])
        return sorted(events)

    def get_event_versions(self, vendor, event_name):
        """
        Get a list of registered versions for a given event schema
        :param vendor:
        :param event_name:
        :return:
        """
        versions = []
        for schema in self.event_schemas:
            if schema['vendor'] == vendor and schema['name'] == event_name:
                for deployment in schema['deployments']:
                    versions.append(deployment['version'])
        return versions

    def get_event_schema_hash(self, vendor, event_name):
        """
        Gets the hash associated with a registered schema by vendor and event_name
        :param vendor:
        :param event_name:
        :return:
        """
        for schema in self.event_schemas:
            if schema['vendor'] == vendor and schema['name'] == event_name:
                return schema['hash']
        return f'No matching event schema found: {vendor}:{event_name}'

    def get_event_jsonschema(self, vendor, event_name, version):
        """
        Gets the registered JSON schema for a given vendor, event_name, and version
        :param vendor:
        :param event_name:
        :param version:
        :return:
        """
        schema_hash = self.get_event_schema_hash(vendor, event_name)
        url = f'{self.api_url_stub}/data-structures/v1/{schema_hash}/versions/{version}'
        headers = {
            'accept': 'application/json',
            'Authorization': self.bearer_token
        }
        resp = requests.get(url, headers=headers)

        try:
            return resp.json()
        except Exception as e:
            return resp.text

    # ---- now the fun parts
    def throw_snowball(self, endpoint, payload):
        """

        """
        print(f'-- Emitting event to Tracker: {payload}')
        num_success = len(self.success)
        num_fail = len(self.fail)

        resp = self.tracker.track_self_describing_event(SelfDescribingJson(
            endpoint,
            payload
        ))
        if len(self.success) > num_success:
            print(f"-- SUCCESS: tracked `good` event_id: [{self.success[-1]['eid']}]\n")
        else:
            print(f"-- FAILED: tracked `bad` event_id: [{self.fail[-1]['eid']}]\n")

        return resp

    def generate_snowballs_by_event(self, vendor, name, version, num_events: int = 1):
        """
        Uses a SnowballFactory to generate a list of random JSON payloads
        for a registered schema
        """
        schema = json.dumps(self.get_event_jsonschema(vendor, name, version))
        factory = SnowplowSnowballFactory(schema)
        snowballs = factory.generate_stash_of_snowballs(num_events).to_json(orient="records")
        return json.loads(snowballs)

    def start_snowball_fight(self, vendor, name, version, num_events: int = 1):
        """

        """
        self.success = []
        self.fail = []
        endpoint = f'iglu:{vendor}/{name}/jsonschema/{version}'

        print('-------- BEGIN EVENT COLLECTION --------')
        print(f'-- Generating {num_events} JSON event payloads for schema: {endpoint}')
        snowballs = self.generate_snowballs_by_event(vendor, name, version, num_events)

        throws = []
        for snowball in snowballs:
            throws.append(self.throw_snowball(endpoint, snowball))
        print(f'-- Snowball Fight complete!')
        print('-------- END EVENT COLLECTION ---------')
        return throws
