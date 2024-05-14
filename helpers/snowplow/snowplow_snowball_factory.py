import json
import uuid
import requests
import pandas as pd
import random as rand
import datetime as dt
import logging as log


def get_snowplow_event_schema_template(desc, vendor, name, version, add_props=True):
    """
    Baseline template required for all new or updated Snowplow events; once fully populated with
    event properties, this is what gets saved in Iglu schema manager to pass event data
    through in the expected formats.
    :param desc:
    :param vendor: tells us who created this JSON Schema
    :param name: name of the event JSON schema being tracked
    :param version: schemaVer
    :param add_props: false means any events sent with properties not defined in the schema will fail validation;
        if true, the events will pass validation but properties not in the schema will be archived rather than loaded

    :return: JSON iglu schema template, without property fields filled in
    """
    template = {
        "$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
        "description": desc,
        "self": {
            "vendor": vendor,
            "name": name,
            "format": "jsonschema",
            "version": version
        },
        "type": "object",
        "properties": {},
        "additionalProperties": add_props
    }

    return template


def is_schema_json(event_schema):
    """
    Very simple logical check to see if value is valid JSON
    """
    try:
        json.loads(event_schema)
    except ValueError as e:
        raise ValueError(f'Please enter a Snowplow event schema with valid JSON:\n{e}')
    return True


def get_field_type_from_schema(prop_schema, field):
    """

    :param prop_schema:
    :param field:
    :return:
    """
    # determine type information for each property, treating unknown types as strings
    if prop_schema == {}:
        dtype = 'string'
    else:
        try:
            if isinstance(prop_schema['type'], list):
                dtype = prop_schema['type'][0]
            else:
                dtype = prop_schema['type']
        except Exception as e:
            dtype = 'string'

    return dtype


def get_max_len_from_schema(prop_schema, field):
    """

    """
    if 'maxLength' in prop_schema.keys():
        max_len = prop_schema['maxLength']
    else:
        max_len = None

    return max_len


def check_fields_for_structures(fields):
    """
    Performs a check to determine if a property field of a schema contains a nested structure
    :param fields:
    :return:
    """
    structs = []
    for field in fields:
        check_struct = field.split()
        if check_struct:
            fields.remove(field)
            structs.append(check_struct)

    return fields, structs


def get_random_words():
    # TODO: replace and find a better way
    word_site = "https://www.mit.edu/~ecprice/wordlist.10000"
    response = requests.get(word_site)
    return response.content.splitlines()


class SnowplowSnowballFactory:
    def __init__(self, event_schema: str = None) -> None:
        # TODO: expand support to include entities in addition to events

        if is_schema_json(event_schema):
            # TODO: add https://github.com/snowplow-incubator/dsctl validation on input schema
            self.event_schema = json.loads(event_schema)
            self.properties = self.event_schema['properties']
            self.name = self.event_schema['self']['name']
            self.version = self.event_schema['self']['version']
            self.vendor = self.event_schema['self']['vendor']

            self.random_words = get_random_words()
        else:
            log.info('Either no schema or an invalid schema was entered. Initializing helper without a skeleton.')

    def generate_random_field_value(self, field, dtype, str_len):
        """
        Populates a very random, very junk value attempting to conform to known dtypes
        :param field: name of the field that needs a random junk value
        :param dtype: date type of the field to generate a value for
        :return: a completely made up junk value
        """
        # start by assigning a random string value
        rand_val = self.random_words[rand.randint(0, 9000)].decode()

        # fill properties with random values acceptable for known types
        if dtype == 'string' and (field.endswith('Timestamp') or field.endswith('Time')):
            # current timestamp for timestamp fields
            rand_val = str(dt.datetime.now())
        elif dtype == 'string' and (field.endswith('ID') or field.endswith('Id')):
            # random uuid for id and uuid fields
            rand_val = str(uuid.uuid4())
        elif dtype == 'integer':
            # random number between 1 and 4000 for and integer fields
            rand_val = rand.randint(1, 4000)
        elif dtype in ['float', 'number']:
            # random 2 decimal floating point number for any number fields
            rand_val = rand.randint(100, 999) / 100.00
        elif dtype == 'boolean':
            # randomly fill with true or false when the field is a boolean
            rand_val = bool(rand.randint(0, 1))

        # add some nuiance to the random value fills when the expected format for a specific field is known
        if field.startswith('numOf'):
            # smaller random integer values that are more appropriate for levels/quantities
            rand_val = rand.randint(1, 10)
        elif field in ['userXP', 'xpGained', 'userScore'] or field.endswith('blankoExperience'):
            # larger random integer values that are more appropriate for scores/experience
            rand_val = rand.randint(1, 999999)
        elif field in ['refreshRate', 'screenResolutionX', 'screenResolutionY']:
            # shifted integer range that could be slightly more believable for screen related numbers
            rand_val = rand.randint(100, 4000)
        elif field.endswith('Version'):
            # random triplicate similar to many versioning systems
            v = f"{rand.randint(1, 9)}.{rand.randint(1, 9)}.{rand.randint(1, 9)}"
            rand_val = v
        elif field == 'changelist':
            # 0.33.x random triplicate aligning with changelist value expectations
            v = f"0.33.{rand.randint(1, 99)}"
            rand_val = v
        elif field == 'platform':
            # games are currently on limited platforms, so choose from a random dummy enum list
            platforms = ['MAC_CLIENT', 'PC_CLIENT']
            rand_val = platforms[rand.randint(0, len(platforms) - 1)]
        elif field == 'operatingSystem':
            # games are currently on limited platforms, so choose from a random dummy enum list
            os = ['WINDOWS', 'MAC_OS', 'LINUX']
            rand_val = os[rand.randint(0, len(os) - 1)]
        elif field == 'userLanguage':
            lang = ['EN', 'ES', 'DE', 'FR']
            rand_val = lang[rand.randint(0, len(lang) - 1)]
        elif field == 'userLocale':
            # games are currently on limited platforms, so choose from a random dummy enum list
            loc = ['en_US', 'en_GB']
            rand_val = loc[rand.randint(0, len(loc) - 1)]
        elif field == 'externalIp':
            # randomly generated IPv4 address
            rand_val = f"{rand.randint(1, 255)}.{rand.randint(1, 255)}.{rand.randint(1, 255)}.{rand.randint(1, 255)}"

        # truncate string value so it's always less than max length
        if dtype == 'string' and str_len is not None and len(rand_val) > str_len:
            rand_val = rand_val[:str_len]

        return rand_val

    def generate_nested_property_values(self, prop_schema):
        """
        This looks like a job for recursion!
        This looks like a job for recursion!
                   like a job for recursion!
                              for recursion!
                                  recursion!

        ... unless it's an array, then YMMV
        """
        payload = {}

        for i in prop_schema.keys():
            i_type = get_field_type_from_schema(prop_schema[i], i)
            i_len = get_max_len_from_schema(prop_schema[i], i)

            if i in ['items', 'virtualCurrencies']:
                # the items and virtualCurrencies properties types are set to object, but they are actually arrays
                # this is a fragile piece of logic as we scale, but arrays complicate snowplow schema levels
                payload[i] = []

                # randomly generate between 1 and 5 objects for the array
                n_objs = rand.randint(1, 5)
                for n in range(n_objs):
                    payload[i].append(self.generate_nested_property_values(prop_schema[i]['properties']))

            elif i_type == 'object':
                if set(prop_schema[i].keys()) == {'description', 'type', 'properties'}:
                    payload[i] = self.generate_nested_property_values(prop_schema[i]['properties'])
                else:
                    payload[i] = prop_schema[i]
            else:
                payload[i] = self.generate_random_field_value(i, i_type, i_len)

        return payload

    def generate_snowball_payload(self, prop_schema):
        """
        A Snowball is a fake payload matching the structure of a custom JSON Snowplow event,
        meant to be 'thrown' somewhere random for testing purposes
        :param prop_schema:
        :return: Snowball event JSON payload
        """
        payload = self.generate_nested_property_values(prop_schema)
        payload['endpoint'] = f'iglu:{self.vendor}/{self.name}/jsonschema/{self.version}'
        return payload

    def generate_stash_of_snowballs(self, n: int):
        """
        Creates a pandas DataFrame containing n-number of fake
        event snowballs that can be used or evaluated in sequence
        :param n: the number of snowballs you'd like to generate
        :return: DataFrame containing fake event(s) data
        """
        payloads = []

        for i in range(n):
            payload = self.generate_snowball_payload(self.properties)
            payloads.append(payload)

        return pd.DataFrame.from_records(payloads)
