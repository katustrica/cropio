import gettext
import configparser
from dataclasses import dataclass

import requests
from marshmallow import Schema, fields, EXCLUDE, post_load, validate


ru = gettext.translation('base', localedir='locales', languages=['ru'])
ru.install()

config = configparser.ConfigParser()
config.read('config.ini')
token = config['maxim']['token']


drivers_url = 'https://cropio.com/api/v3/users'
tasks_url = 'https://cropio.com/api/v3/machine_tasks'
field_mapping_url = 'https://cropio.com/api/v3/machine_task_field_mapping_items'

driver_filter = '?driver_id={}'
task_filter = '?machine_task_id={}'

@dataclass
class Driver():
    id: int
    username: str


class UserSchema(Schema):
    id = fields.Integer()
    username = fields.Str()
    is_driver = fields.Boolean()

    @post_load
    def make_driver(self, data, **kwargs):
        return Driver(data['id'], data['username']) if data['is_driver'] else None


@dataclass
class Task():
    id: int
    machine_id: int
    start_time: int


class TaskSchema(Schema):
    username = fields.Str()
    driver = fields.Boolean()
    id = fields.Integer()

    @post_load
    def make_driver(self, data, **kwargs):
        return Driver(data['id'], data['username']) if data['driver'] else None


request_fields = requests.get(drivers_url, headers={"X-User-Api-Token": token}).json()
drivers = [d for d in UserSchema(many=True).load(request_fields['data'], unknown=EXCLUDE) if d]
import pdb; pdb.set_trace()

