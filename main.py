import gettext
import configparser
from pprint import pprint
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import logging
import json

import requests
from marshmallow import Schema, fields, EXCLUDE, post_load, validate
import pandas as pd
from tqdm import tqdm
import gspread
import df2gspread as d2g
from pathlib import Path

from schema import (DriverSchema, MachineSchema, ImplementSchema, WorkTypeGroupSchema, WorkTypeSchema,
                    TaskFieldMappingSchema, TaskSchema, FieldSchema)
from data import TaskForUser


logging.basicConfig(level=20)
ru = gettext.translation('base', localedir='locales', languages=['ru'])
ru.install()

# configs
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

config_path = Path(config['cache']['path'])
if not config_path.exists():
    config_path.mkdir(parents=True, exist_ok=False)
tasks_path = config_path / Path(config['cache']['tasks'])
task_fields_path = config_path / Path(config['cache']['task_fields'])
drivers_path = config_path / Path(config['cache']['drivers'])
work_types_path = config_path / Path(config['cache']['work_types'])
work_type_groups_path = config_path / Path(config['cache']['work_type_groups'])
machines_path = config_path / Path(config['cache']['machines'])
implements_path = config_path / Path(config['cache']['implements'])

ids = '/ids'
updated_filter = '?updated_at_gt_eq={}'
driver_filter = '?driver={}'
id_filter = '?id={}'
task_filter = '?machine_task_id={}'


def get_machines():
    machines = []
    available_machine_ids = []
    last_updated_max = datetime(1000, 1, 1, 1, 1, 1, 1, datetime.now().astimezone().tzinfo)
    machine_ids = requests.get(machines_url + ids,
                               headers={"X-User-Api-Token": token}).json()['data']

    logging.info('Проверка обновленных машин:')
    if machines_path.exists():
        with(machines_path.open('r+')) as outfile:
            machines = MachineSchema(many=True).loads(outfile.read())

        for machine in tqdm(machines):
            last_updated_max = [last_updated_max, machine.updated_at][machine.updated_at > last_updated_max]
            available_machine_ids.append(machine.id)

    last_updated_max_str = last_updated_max.strftime('%Y-%m-%dT%H:%M:%SZ')
    updated_machine_ids = requests.get(machines_url + ids + updated_filter.format(last_updated_max_str),
                                       headers={"X-User-Api-Token": token}).json()['data']
    missed_machine_ids = list(set(machine_ids) - set(updated_machine_ids) - set(available_machine_ids))

    logging.info('Обновление машин:')
    for updated_machine_id in tqdm(updated_machine_ids + missed_machine_ids):
        request_updated_machine_json = requests.get(machines_url + f'/{updated_machine_id}',
                                                    headers={"X-User-Api-Token": token}).json()['data']
        updated_machine = MachineSchema().load(request_updated_machine_json)
        if updated_machine.id in available_machine_ids:
            old_machine_index = [machine.id for machine.id in machines if machine.id == updated_machine.id][0]
            machines[old_machine_index] = updated_machine
        else:
            machines.append(updated_machine)

    # write machine to json
    logging.info('Запись в кэш.')
    with(machines_path.open('w+')) as outfile:
        outfile.write(MachineSchema(many=True).dumps(machines))
    return {m.id: m for m in machines}


def get_implements():
    implements = []
    available_implement_ids = []
    last_updated_max = datetime(1000, 1, 1, 1, 1, 1, 1, datetime.now().astimezone().tzinfo)
    implement_ids = requests.get(implements_url + ids,
                                 headers={"X-User-Api-Token": token}).json()['data']

    logging.info('Проверка обновленных агрегатов:')
    if implements_path.exists():
        with(implements_path.open('r+')) as outfile:
            implements = ImplementSchema(many=True).loads(outfile.read())

        for implement in tqdm(implements):
            last_updated_max = [last_updated_max, implement.updated_at][implement.updated_at > last_updated_max]
            available_implement_ids.append(implement.id)

    last_updated_max_str = last_updated_max.strftime('%Y-%m-%dT%H:%M:%SZ')
    updated_implement_ids = requests.get(implements_url + ids + updated_filter.format(last_updated_max_str),
                                       headers={"X-User-Api-Token": token}).json()['data']
    missed_implement_ids = list(set(implement_ids) - set(updated_implement_ids) - set(available_implement_ids))

    logging.info('Обновление агрегатов:')
    for updated_implement_id in tqdm(updated_implement_ids + missed_implement_ids):
        request_updated_implement_json = requests.get(implements_url + f'/{updated_implement_id}',
                                                    headers={"X-User-Api-Token": token}).json()['data']
        updated_implement = ImplementSchema().load(request_updated_implement_json)
        if updated_implement.id in available_implement_ids:
            old_implement_index = [implement.id for implement.id in implements if implement.id == updated_implement.id][0]
            implements[old_implement_index] = updated_implement
        else:
            implements.append(updated_implement)

    # write implement to json
    logging.info('Запись в кэш.')
    with(implements_path.open('w+')) as outfile:
        outfile.write(ImplementSchema(many=True).dumps(implements))
    return {i.id: i for i in implements}


# WorkTypeGroup
def get_work_type_groups():
    work_type_groups = []
    available_work_type_group_ids = []
    last_updated_max = datetime(1000, 1, 1, 1, 1, 1, 1, datetime.now().astimezone().tzinfo)
    work_type_group_ids = requests.get(work_type_groups_url + ids,
                                       headers={"X-User-Api-Token": token}).json()['data']

    logging.info('Проверка обновленных типов групп заданий:')
    if work_type_groups_path.exists():
        with(work_type_groups_path.open('r+')) as outfile:
            work_type_groups = WorkTypeGroupSchema(many=True).loads(outfile.read())

        for work_type_group in tqdm(work_type_groups):
            last_updated_max = [
                last_updated_max, work_type_group.updated_at
            ][work_type_group.updated_at > last_updated_max]
            available_work_type_group_ids.append(work_type_group.id)

    last_updated_max_str = last_updated_max.strftime('%Y-%m-%dT%H:%M:%SZ')
    updated_work_type_group_ids = requests.get(
        work_type_groups_url + ids + updated_filter.format(last_updated_max_str),
        headers={"X-User-Api-Token": token}
    ).json()['data']
    missed_work_type_group_ids = list(set(work_type_group_ids)
                                      - set(updated_work_type_group_ids)
                                      - set(available_work_type_group_ids))

    logging.info('Обновление типов групп работ:')
    for updated_work_type_group_id in tqdm(updated_work_type_group_ids + missed_work_type_group_ids):
        request_updated_work_type_group_json = requests.get(
            work_type_groups_url + f'/{updated_work_type_group_id}',
            headers={"X-User-Api-Token": token}
        ).json()['data']
        updated_work_type_group = WorkTypeGroupSchema().load(request_updated_work_type_group_json)
        if updated_work_type_group.id in available_work_type_group_ids:
            old_work_type_group_index = [
                work_type_group.id for work_type_group.id in work_type_groups
                if work_type_group.id == updated_work_type_group.id
            ][0]
            work_type_groups[old_work_type_group_index] = updated_work_type_group
        else:
            work_type_groups.append(updated_work_type_group)

    # write work_type_group to json
    logging.info('Запись в кэш.')
    with(work_type_groups_path.open('w+')) as outfile:
        outfile.write(WorkTypeGroupSchema(many=True).dumps(work_type_groups))
    return {w.id: w for w in work_type_groups}


# WorkType
def get_work_types():
    work_types = []
    available_work_type_ids = []
    last_updated_max = datetime(1000, 1, 1, 1, 1, 1, 1, datetime.now().astimezone().tzinfo)
    work_type_ids = requests.get(work_types_url + ids,
                                 headers={"X-User-Api-Token": token}).json()['data']

    logging.info('Проверка обновленных типов заданий:')
    if work_types_path.exists():
        with(work_types_path.open('r+')) as outfile:
            work_types = WorkTypeSchema(many=True).loads(outfile.read())

        for work_type in tqdm(work_types):
            last_updated_max = [
                last_updated_max, work_type.updated_at
            ][work_type.updated_at > last_updated_max]
            available_work_type_ids.append(work_type.id)

    last_updated_max_str = last_updated_max.strftime('%Y-%m-%dT%H:%M:%SZ')
    updated_work_type_ids = requests.get(work_types_url + ids + updated_filter.format(last_updated_max_str),
                                         headers={"X-User-Api-Token": token}).json()['data']
    missed_work_type_ids = list(set(work_type_ids) - set(updated_work_type_ids) - set(available_work_type_ids))

    logging.info('Обновление типов работ:')
    for updated_work_type_id in tqdm(updated_work_type_ids + missed_work_type_ids):
        request_updated_work_type_json = requests.get(work_types_url + f'/{updated_work_type_id}',
                                                      headers={"X-User-Api-Token": token}).json()['data']
        updated_work_type = WorkTypeSchema().load(request_updated_work_type_json)
        if updated_work_type.id in available_work_type_ids:
            old_work_type_index = [
                work_type.id for work_type.id in work_types if work_type.id == updated_work_type.id
            ][0]
            work_types[old_work_type_index] = updated_work_type
        else:
            work_types.append(updated_work_type)

    # write work_type to json
    logging.info('Запись в кэш.')
    with(work_types_path.open('w+')) as outfile:
        outfile.write(WorkTypeSchema(many=True).dumps(work_types))
    return {w.id : w for w in work_types}


# Fields
def get_field_name(field_id):
    request_fields_json = requests.get(fields_url + id_filter.format(field_id),
                                       headers={"X-User-Api-Token": token}).json()['data'][0]
    return FieldSchema().load(request_fields_json).name


# Drivers
def get_drivers():
    drivers = []
    available_driver_ids = []
    last_updated_max = datetime(1000, 1, 1, 1, 1, 1, 1, datetime.now().astimezone().tzinfo)
    driver_ids = requests.get(drivers_url + ids,
                              headers={"X-User-Api-Token": token}).json()['data']

    logging.info('Проверка обновленных водителей:')
    if drivers_path.exists():
        with(drivers_path.open('r+')) as outfile:
            drivers = DriverSchema(many=True).loads(outfile.read())

        for driver in tqdm(drivers):
            last_updated_max = [last_updated_max, driver.updated_at][driver.updated_at > last_updated_max]
            available_driver_ids.append(driver.id)

    last_updated_max_str = last_updated_max.strftime('%Y-%m-%dT%H:%M:%SZ')
    updated_driver_ids = requests.get(drivers_url + ids + updated_filter.format(last_updated_max_str),
                                      headers={"X-User-Api-Token": token}).json()['data']
    missed_driver_ids = list(set(driver_ids) - set(updated_driver_ids) - set(available_driver_ids))

    logging.info('Обновление водителей:')
    for updated_driver_id in tqdm(updated_driver_ids + missed_driver_ids):
        request_updated_driver_json = requests.get(drivers_url + f'/{updated_driver_id}',
                                                   headers={"X-User-Api-Token": token}).json()['data']
        updated_driver = DriverSchema().load(request_updated_driver_json)
        if updated_driver.id in available_driver_ids:
            old_driver_index = [driver.id for driver.id in drivers if driver.id == updated_driver.id][0]
            drivers[old_driver_index] = updated_driver
        else:
            drivers.append(updated_driver)

    # write driver to json
    logging.info('Запись в кэш.')
    with(drivers_path.open('w+')) as outfile:
        outfile.write(DriverSchema(many=True).dumps(drivers))
    result = {d.id: d for d in drivers}
    return result


# TaskFieldMapping
def get_task_field_mapping():
    task_fields = []
    available_task_field_ids = []
    last_updated_max = datetime(1000, 1, 1, 1, 1, 1, 1, datetime.now().astimezone().tzinfo)
    task_field_ids = requests.get(task_field_mapping_url + ids,
                                  headers={"X-User-Api-Token": token}).json()['data']
    task_ids = requests.get(tasks_url + ids,
                            headers={"X-User-Api-Token": token}).json()['data']

    logging.info('Проверка информации о полях в заданиях:')
    if task_fields_path.exists():
        with(task_fields_path.open('r+')) as outfile:
            task_fields = TaskFieldMappingSchema(many=True).loads(outfile.read())

        for task_field in tqdm(task_fields):
            last_updated_max = [last_updated_max, task_field.updated_at][task_field.updated_at > last_updated_max]
            available_task_field_ids.append(task_field.id)

    last_updated_max_str = last_updated_max.strftime('%Y-%m-%dT%H:%M:%SZ')
    updated_task_field_ids = requests.get(
        task_field_mapping_url + ids + updated_filter.format(last_updated_max_str),
        headers={"X-User-Api-Token": token}
    ).json()['data']
    missed_task_field_ids = (set(task_field_ids)
                             - set(updated_task_field_ids)
                             - set(available_task_field_ids))

    logging.info('Обновление информации о полях в заданиях:')
    needed_ids = set(updated_task_field_ids) | missed_task_field_ids - set(available_task_field_ids)
    if needed_ids:
        for task_id in tqdm(task_ids):
            request_task_field_json = requests.get(task_field_mapping_url + task_filter.format(task_id),
                                                   headers={"X-User-Api-Token": token}).json()['data']
            task_field_loaded = TaskFieldMappingSchema(many=True).load(request_task_field_json)
            for updated_task_field in [t for t in task_field_loaded if t.id in needed_ids]:
                if updated_task_field.id in available_task_field_ids:
                    old_task_field_index = [
                        task_field.id for task_field.id in task_fields if task_field.id == updated_task_field.id
                    ][0]
                    task_fields[old_task_field_index] = updated_task_field
                else:
                    task_fields.append(updated_task_field)

        # write task_field to json
        logging.info('Запись в кэш.')
        with(task_fields_path.open('w+')) as outfile:
            outfile.write(TaskFieldMappingSchema(many=True).dumps(task_fields))

    task_field_mapping = {t: [] for t in task_ids}
    for task_field in task_fields:
        if task_field.covered_area > 0:
            task_field_mapping[task_field.machine_task_id].append(task_field.field_id)

    result = {
       task_id: ", ".join([str(t) for t in task_field_mapping[task_id]])
       for task_id in task_field_mapping
    }
    return result


# Tasks
def get_tasks():
    tasks = []
    available_task_ids = []
    last_updated_max = datetime(1000, 1, 1, 1, 1, 1, 1, datetime.now().astimezone().tzinfo)
    task_ids = requests.get(tasks_url + ids,
                            headers={"X-User-Api-Token": token}).json()['data']

    logging.info('Проверка обновленных заданий:')
    if tasks_path.exists():
        with(tasks_path.open('r+')) as outfile:
            tasks = TaskSchema(many=True).loads(outfile.read())

        for task in tqdm(tasks):
            last_updated_max = [last_updated_max, task.updated_at][task.updated_at > last_updated_max]
            available_task_ids.append(task.id)

    last_updated_max_str = last_updated_max.strftime('%Y-%m-%dT%H:%M:%SZ')
    updated_task_ids = requests.get(tasks_url + ids + updated_filter.format(last_updated_max_str),
                                    headers={"X-User-Api-Token": token}).json()['data']
    missed_task_ids = list(set(task_ids) - set(updated_task_ids) - set(available_task_ids))

    logging.info('Обновление заданий:')
    for updated_task_id in tqdm(updated_task_ids + missed_task_ids):
        request_updated_task_json = requests.get(tasks_url + f'/{updated_task_id}',
                                                 headers={"X-User-Api-Token": token}).json()['data']
        updated_task = TaskSchema().load(request_updated_task_json)
        if updated_task.id in available_task_ids:
            old_task_index = [task.id for task.id in tasks if task.id == updated_task.id][0]
            tasks[old_task_index] = updated_task
        else:
            tasks.append(updated_task)

    # write task to json
    logging.info('Запись в кэш.')
    with(tasks_path.open('w+')) as outfile:
        outfile.write(TaskSchema(many=True).dumps(tasks))
    return tasks


dicts_for_user = {}
tasks = get_tasks()
task_fields = get_task_field_mapping()
drivers = get_drivers()
work_types = get_work_types()
work_type_groups = get_work_type_groups()
machines = get_machines()
implements = get_implements()

for task in tqdm(tasks):
    machine = machines[task.machine_id].name
    driver = drivers[task.driver_id].username if task.driver_id else "Нет водителя"
    implement = implements[task.implement_id].name if task.implement_id else 'Нет агрегата'

    work_type = work_types[task.work_type_id]
    work_type_group = work_type_groups[work_type.work_type_group_id]
    work_msg_for_user = work_type_group.name + ' / ' + work_type.name

    field = task_fields[task.id]
    task_for_user = task.dict_for_user(machine, implement, field, work_msg_for_user)
    if driver in dicts_for_user:
        dicts_for_user[driver].append(task_for_user)
    else:
        dicts_for_user[driver] = [task_for_user]

dfs = []
for driver in tqdm(sorted(dicts_for_user)):
    df = pd.DataFrame.from_dict(dicts_for_user[driver])
    top_row = pd.DataFrame({
        'Дата': [driver], 'Дневная смена': [''], 'Ночная смена': [''], 'Техника': [''], 'Агрегат': [''],
        'Номер полей': [''], 'Операция': [''], 'Расход топлива': [''], 'Выработка (га)': [''],
        'Выработка (км)': ['']
    })
    df['Дата'] = pd.to_datetime(df['Дата'], format='%Y-%m-%d')
    df = df.sort_values(by='Дата')
    df['Дата'] = df['Дата'].dt.strftime('%d/%m/%Y')
    df = pd.concat([top_row, df]).reset_index(drop=True)
    dfs.append(df)
result_df = pd.concat(dfs)


def post_to_google_sheet(result_df):
    json_name = 'oauth.json'
    spreadsheet_key = config['sheets']['musa']
    scope = ['https://www.googleapis.com/auth/spreadsheets']

    creds = ServiceAccountCredentials.from_json_keyfile_name(json_name, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(spreadsheet_key).sheet1
    column_names = [result_df.columns.tolist()]
    rows = result_df.values.tolist()
    sheet.clear()
    sheet.append_rows(column_names + rows)


post_to_google_sheet(result_df)

