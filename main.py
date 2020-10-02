import gettext
import configparser
from pprint import pprint
from datetime import datetime

import requests
from marshmallow import Schema, fields, EXCLUDE, post_load, validate
import pandas as pd

from schema import (DriverSchema, MachineSchema, ImplementSchema, WorkTypeGroupSchema, WorkTypeSchema,
                    TaskFieldMappingSchema, TaskSchema, FieldSchema)
from data import TaskForUser

ru = gettext.translation('base', localedir='locales', languages=['ru'])
ru.install()

config = configparser.ConfigParser()
config.read('config.ini')
token = config['token']['maxim']
fields_url = config['urls']['fields']
drivers_url = config['urls']['drivers']
machines_url = config['urls']['machines']
implements_url = config['urls']['implements']
work_type_groups_url = config['urls']['work_type_groups']
work_types_url = config['urls']['work_types']
tasks_url = config['urls']['tasks']
task_field_mapping_url = config['urls']['task_field_mapping']

driver_filter = '?driver={}'
id_filter = '?id={}'
task_filter = '?machine_task_id={}'

# Machines
request_machines_json = requests.get(machines_url,
                                     headers={"X-User-Api-Token": token}).json()['data']
machines = {k: v for d in MachineSchema(many=True).load(request_machines_json) for k, v in d.items()}

# Implements
request_implements_json = requests.get(implements_url,
                                       headers={"X-User-Api-Token": token}).json()['data']
implements = {k: v for d in ImplementSchema(many=True).load(request_implements_json) for k, v in d.items()}

# WorkTypeGroup
request_work_type_groups_json = requests.get(work_type_groups_url,
                                             headers={"X-User-Api-Token": token}).json()['data']
work_type_groups = {
    k: v for d in WorkTypeGroupSchema(many=True).load(request_work_type_groups_json) for k, v in d.items()
}

# WorkType
request_work_type_json = requests.get(work_types_url,
                                      headers={"X-User-Api-Token": token}).json()['data']
work_types = {k: v for d in WorkTypeSchema(many=True).load(request_work_type_json) for k, v in d.items()}


# Fields
def get_field_name(field_id):
    request_fields_json = requests.get(fields_url + id_filter.format(field_id),
                                       headers={"X-User-Api-Token": token}).json()['data'][0]
    return FieldSchema().load(request_fields_json).name


# Drivers
def get_driver_name(driver_id):
    request_drivers_json = requests.get(drivers_url + id_filter.format(driver_id),
                                        headers={"X-User-Api-Token": token}).json()['data'][0]
    return DriverSchema().load(request_drivers_json).username


# TaskFieldMapping
def get_task_field_mapping(task_id):
    request_task_field_mapping_json = requests.get(task_field_mapping_url + task_filter.format(task_id),
                                                   headers={"X-User-Api-Token": token}).json()['data']
    task_field_mapping = TaskFieldMappingSchema(many=True).load(request_task_field_mapping_json)

    result = ", ".join([get_field_name(t.field_id) for t in task_field_mapping if t])
    return result


# Tasks
request_task_json = requests.get(tasks_url,
                                 headers={"X-User-Api-Token": token}).json()['data']
tasks = TaskSchema(many=True).load(request_task_json)

dicts_for_user = {}
for task in tasks:
    machine = machines[task.machine_id].name
    driver = get_driver_name(task.driver_id) if task.driver_id else "Нет водителя"
    implement = implements[task.implement_id].name if task.implement_id else 'Нет агрегата'
    field = get_task_field_mapping(task.id)
    task_for_user = task.dict_for_user(machine, implement, field)
    if driver in dicts_for_user:
        dicts_for_user[driver].append(task_for_user)
    else:
        dicts_for_user[driver] = [task_for_user]
dfs = []
for driver in dicts_for_user:
    df = pd.DataFrame.from_dict(dicts_for_user[driver])
    top_row = pd.DataFrame({
        'id': [driver], 'date': [''], 'machine': [''], 'day_shift': [''], 'night_shift': [''], 'fuel_consumption': [''],
        'covered_area': [''], 'distance': [''], 'implement': [''], 'fields': ['']
    })
    df = pd.concat([top_row, df]).reset_index(drop=True)
    dfs.append(df)
result_df = pd.concat(dfs)
result_df.to_excel(f'./result-{datetime.now().strftime(r"%d/%m/%Y/%H/%M")}.xlsx', index=False)
import pdb; pdb.set_trace()
