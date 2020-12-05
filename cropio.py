import configparser
from pprint import pprint
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import logging
import json
from typing import List
import string
import math
import os


import requests
from marshmallow import Schema, EXCLUDE, post_load, validate
from marshmallow import fields as m_fields
import pandas as pd
from tqdm import tqdm
import gspread
import df2gspread as d2g
from gspread_formatting import cellFormat, Borders, Border, textFormat, format_cell_ranges, format_cell_range
from pathlib import Path

from schema import (DriverSchema, MachineSchema, ImplementSchema, WorkTypeGroupSchema, WorkTypeSchema,
                    TaskFieldMappingSchema, TaskSchema, FieldSchema)


logging.basicConfig(level=20)

# configs
config = configparser.ConfigParser()
config.read('config.ini')


settings_path = Path(config['settings']['path'])
drivers_excel = settings_path / Path(config['settings']['drivers_excel'])
prices_excel = settings_path / Path(config['settings']['prices_excel'])

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
fields_path = config_path / Path(config['cache']['fields'])
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
time_filter = '?start_time_gt_eq={}&start_time_lt_eq={}'

col_user_key = ['Операция', 'Техника', 'Агрегат']
col_user_show = ['Выработка (га)', 'Выработка (км)', 'Номер полей',
                 'Дневная смена', 'Ночная смена', 'Простой', 'Работа (руб.)', 'Перегон (руб.)']
col_user_show_sum = ['Дневная смена', 'Ночная смена', 'Работа (руб.)', 'Выработка (га)', 'Выработка (км)', 'Перегон (руб.)']


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
            machines[available_machine_ids.index(updated_machine.id)] = updated_machine
        else:
            available_machine_ids.append(updated_machine.id)
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
            implements[available_implement_ids.index(updated_implement.id)] = updated_implement
        else:
            available_implement_ids.append(updated_implement.id)
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
            index = available_work_type_group_ids.index(updated_work_type_group.id)
            work_type_groups[index] = updated_work_type_group
        else:
            available_work_type_group_ids.append(updated_work_type_group.id)
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
            work_types[available_work_type_ids.index(updated_work_type.id)] = updated_work_type
        else:
            available_work_type_ids.append(updated_work_type.id)
            work_types.append(updated_work_type)

    # write work_type to json
    logging.info('Запись в кэш.')
    with(work_types_path.open('w+')) as outfile:
        outfile.write(WorkTypeSchema(many=True).dumps(work_types))
    return {w.id : w for w in work_types}


# Fields
def get_fields():
    fields = []
    available_field_ids = []
    last_updated_max = datetime(1000, 1, 1, 1, 1, 1, 1, datetime.now().astimezone().tzinfo)
    field_ids = requests.get(fields_url + ids,
                             headers={"X-User-Api-Token": token}).json()['data']

    logging.info('Проверка обновленных полей:')
    if fields_path.exists():
        with(fields_path.open('r+')) as outfile:
            fields = FieldSchema(many=True).loads(outfile.read())

        for field in tqdm(fields):
            last_updated_max = [last_updated_max, field.updated_at][field.updated_at > last_updated_max]
            available_field_ids.append(field.id)

    last_updated_max_str = last_updated_max.strftime('%Y-%m-%dT%H:%M:%SZ')
    updated_field_ids = requests.get(fields_url + ids + updated_filter.format(last_updated_max_str),
                                     headers={"X-User-Api-Token": token}).json()['data']
    missed_field_ids = list(set(field_ids) - set(updated_field_ids) - set(available_field_ids))

    logging.info('Обновление полей:')
    for updated_field_id in tqdm(updated_field_ids + missed_field_ids):
        request_updated_field_json = requests.get(fields_url + f'/{updated_field_id}',
                                                  headers={"X-User-Api-Token": token}).json()['data']
        updated_field = FieldSchema().load(request_updated_field_json)
        if updated_field.id in available_field_ids:
            fields[available_field_ids.index(updated_field.id)] = updated_field
        else:
            available_field_ids.append(updated_field.id)
            fields.append(updated_field)

    # write field to json
    logging.info('Запись в кэш.')
    with(fields_path.open('w+')) as outfile:
        outfile.write(FieldSchema(many=True).dumps(fields))
    result = {d.id: d for d in fields}
    return result


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
            drivers[available_driver_ids.index(updated_driver.id)] = updated_driver
        else:
            available_driver_ids.append(updated_driver.id)
            drivers.append(updated_driver)

    # write driver to json
    logging.info('Запись в кэш.')
    with(drivers_path.open('w+')) as outfile:
        outfile.write(DriverSchema(many=True).dumps(drivers))
    result = {d.id: d for d in drivers}
    return result


# TaskFieldMapping
def get_task_field_mapping(fields):
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
            last_updated_max = [
                last_updated_max, task_field.updated_at
            ][task_field.updated_at > last_updated_max]
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
        if len(needed_ids) < len(task_ids):
            for needed_id in tqdm(needed_ids):
                request_task_field_json = requests.get(task_field_mapping_url + id_filter.format(needed_id),
                                                       headers={"X-User-Api-Token": token}).json()['data'][0]
                task_field_loaded = TaskFieldMappingSchema().load(request_task_field_json)
                if task_field_loaded.id in available_task_field_ids:
                    task_fields[available_task_field_ids.index(task_field_loaded.id)] = task_field_loaded
                else:
                    available_task_field_ids.append(task_field_loaded.id)
                    task_fields.append(task_field_loaded)
        else:
            for task_id in tqdm(task_ids):
                request_task_field_json = requests.get(task_field_mapping_url + task_filter.format(task_id),
                                                       headers={"X-User-Api-Token": token}).json()['data']
                task_field_loaded = TaskFieldMappingSchema(many=True).load(request_task_field_json)
                for updated_task_field in [t for t in task_field_loaded if t.id in needed_ids]:
                    if updated_task_field.id in available_task_field_ids:
                        task_fields[available_task_field_ids.index(updated_task_field.id)] = updated_task_field
                    else:
                        available_task_field_ids.append(updated_task_field.id)
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
       task_id: ", ".join([str(fields[t].name) for t in task_field_mapping[task_id]])
       for task_id in task_field_mapping
    }
    return result


# Tasks
def get_tasks(start_date, finish_date):
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
    time_format = '%Y-%m-%dT%H:%M:%SZ'
    last_updated_max_str = last_updated_max.strftime(time_format)
    updated_task_ids = requests.get(tasks_url
                                    + ids
                                    + updated_filter.format(last_updated_max_str),
                                    headers={"X-User-Api-Token": token}).json()['data']
    missed_task_ids = list(set(task_ids) - set(updated_task_ids) - set(available_task_ids))

    logging.info('Обновление заданий:')
    for updated_task_id in tqdm(updated_task_ids + missed_task_ids):
        request_updated_task_json = requests.get(tasks_url + f'/{updated_task_id}',
                                                 headers={"X-User-Api-Token": token}).json()['data']
        updated_task = TaskSchema().load(request_updated_task_json)
        if updated_task.id in available_task_ids:
            tasks[available_task_ids.index(updated_task.id)] = updated_task
        else:
            available_task_ids.append(updated_task.id)
            tasks.append(updated_task)

    # write task to json
    logging.info('Запись в кэш.')
    with(tasks_path.open('w+')) as outfile:
        outfile.write(TaskSchema(many=True).dumps(tasks))
    needed_time_task_ids = requests.get(tasks_url
                                        + ids
                                        + time_filter.format(start_date, finish_date),
                                        headers={"X-User-Api-Token": token}).json()['data']
    result = [tasks[available_task_ids.index(i)] for i in needed_time_task_ids]
    return result


def labels(alphabet=string.ascii_uppercase):
    assert len(alphabet) == len(set(alphabet))  # make sure every letter is unique
    s = [alphabet[0]]
    while 1:
        yield ''.join(s)
        l = len(s)
        for i in range(l-1, -1, -1):
            if s[i] != alphabet[-1]:
                s[i] = alphabet[alphabet.index(s[i])+1]
                s[i+1:] = [alphabet[0]] * (l-i-1)
                break
        else:
            s = [alphabet[0]] * (l+1)


def post_to_google_sheet(strokes, sheet_name):
    json_name = 'oauth.json'
    spreadsheet_key = config['sheets']['musa']
    scope = ['https://www.googleapis.com/auth/spreadsheets']

    creds = ServiceAccountCredentials.from_json_keyfile_name(json_name, scope)
    client = gspread.authorize(creds)
    sh = client.open_by_key(spreadsheet_key)
    cols_num = max(len(s) for s in strokes)
    if sheet_name not in [i.title for i in sh.worksheets()]:
        sh.add_worksheet(title=sheet_name, rows=len(strokes), cols=cols_num)
    sheet = sh.worksheet(sheet_name)
    requests = {"requests": [{"updateCells": {"range": {"sheetId": sheet._properties['sheetId']}, "fields": "*"}}]}
    sh.batch_update(requests)
    logging.info('Публикую строки')

    sheet.update(strokes)

    fmt_center_top_bold = cellFormat(
        textFormat=textFormat(bold=True),
        horizontalAlignment='CENTER',
        borders=Borders(top=(Border(style="SOLID_THICK")))
    )
    fmt_center_bot_bold = cellFormat(
        textFormat=textFormat(bold=True),
        horizontalAlignment='CENTER',
        borders=Borders(bottom=(Border(style="SOLID_MEDIUM")))
    )
    fmt_right_bold = cellFormat(borders=Borders(right=(Border(style="SOLID_THICK"))))
    fmt_left_bold = cellFormat(borders=Borders(left=(Border(style="SOLID_THICK"))))

    x = labels()
    col_names = [next(x) for _ in range(cols_num)]

    logging.info('Смотрю строки для выделения')
    cell_range = [(str(strokes.index(s)+1), fmt_center_top_bold) for s in tqdm(strokes) if len(s) == 1]
    if cell_range != []:
        logging.info('Выделяю строки')
        format_cell_ranges(sheet, cell_range)

    logging.info('Смотрю строки для выделения')
    cell_range = [(c, fmt_right_bold) for c in tqdm(col_names[len(col_user_key)+len(col_user_show_sum)-1::len(col_user_show)])]
    if cell_range != []:
        logging.info('Выделяю строки')
        format_cell_ranges(sheet, cell_range)

    format_cell_range(sheet, col_names[len(col_user_key)-1], fmt_right_bold)
    format_cell_range(sheet, col_names[len(col_user_key)-1], fmt_right_bold)


def update_drivers_xlsx(dicts_for_user: List[str]):
    df = pd.DataFrame(list(dicts_for_user.keys()))
    df.to_excel(drivers_excel, index=False, encoding='cp1251')

def open_drivers():
    os.system(f'start {drivers_excel}')

def load_drivers():
    drivers = get_drivers()
    drivers_set = (d.username for d in drivers.values() if d.driver)
    drivers_data = pd.DataFrame([['Район']] + [[d] for d in drivers_set])
    drivers_data.to_excel(drivers_excel, index=False, encoding='cp1251', header=False)

def open_prices():
    os.system(f'start {prices_excel}')

def table(start_date: str, finish_date: str):
    dicts_for_user = {}

    tasks = get_tasks(start_date, finish_date)
    drivers = get_drivers()
    work_types = get_work_types()
    work_type_groups = get_work_type_groups()
    machines = get_machines()
    implements = get_implements()
    fields = get_fields()
    task_fields = get_task_field_mapping(fields)
    for task in tqdm(tasks):
        machine = machines[task.machine_id].name
        driver = drivers[task.driver_id].username if task.driver_id else "Нет водителя"
        implement = implements[task.implement_id].name if task.implement_id else 'Нет агрегата'

        work_type = work_types[task.work_type_id]
        work_type_group = work_type_groups[work_type.work_type_group_id]
        work_msg_for_user = work_type_group.name + ' / ' + work_type.name
        machine_manufacturer = machines[task.machine_id].manufacturer
        work_type = work_type.name
        field = task_fields[task.id]
        task_for_user = task.dict_for_user(machine,
                                           machine_manufacturer,
                                           work_type,
                                           implement,
                                           field,
                                           work_msg_for_user,
                                           driver)
        if driver in dicts_for_user:
            dicts_for_user[driver].append(task_for_user)
        else:
            dicts_for_user[driver] = [task_for_user]

    drivers_df_csv = pd.read_excel(drivers_excel, encoding='cp1251')
    for aria_name in drivers_df_csv:
        strokes = []
        dates_max = []
        drirvers_in_aria = [i for i in drivers_df_csv[aria_name].to_list() if (isinstance(i, str) and i in dicts_for_user)]
        for driver in tqdm(sorted(drirvers_in_aria)):
            strokes.append([f'{driver}'])
            df = pd.DataFrame.from_dict(dicts_for_user[driver]).round(2)
            df['Дата'] = pd.to_datetime(df['Дата'], format='%Y-%m-%d')
            df = df.sort_values(by='Дата')
            df['Дата'] = df['Дата'].dt.strftime('%d/%m/%Y')
            dates = df['Дата'].to_list()
            dates_max = dates if len(dates) > len(dates_max) else dates_max
            strokes.append([i for j in [['']*len(col_user_key)] + [col_user_show_sum] + [col_user_show for date in dates] for i in j])
            type_of_ops = {t: [] for t in set([tuple(e) for e in df[col_user_key].values.tolist()])}
            type_of_ops_sum = {t: ['']*len(col_user_show_sum) for t in set([tuple(e) for e in df[col_user_key].values.tolist()])}
            # mul_keys = df[col_user_key].values.tolist()
            grouped_df = df.groupby(col_user_key).sum()
            for op in type_of_ops:
                for date in dates:
                    temp_df = df[df['Дата'] == date]
                    if temp_df[col_user_key].values.tolist()[0] == list(op):
                        values = temp_df[col_user_show].values.tolist()[0]
                        type_of_ops[op].append(values)
                        values_sum = temp_df[col_user_show_sum].values.tolist()[0]
                        for i in range(len(type_of_ops_sum[op])):
                            if isinstance(values_sum[i], (int, float)):
                                if type_of_ops_sum[op][i] == '':
                                    type_of_ops_sum[op][i] = values_sum[i]
                                else:
                                    type_of_ops_sum[op][i] += values_sum[i]
                            else:
                                continue
                    else:
                        type_of_ops[op].append([''] * len(col_user_show))
                grouped_df.query(" and ".join([f"{key} == '{d}'" for key, d in zip(col_user_key, op)]))
                temp_list = [list(op)] + [type_of_ops_sum[op]] + type_of_ops[op]
                strokes.append([j for i in temp_list for j in i])

        strokes = [[
            i for j in [['']*len(col_user_key)] + [['Сумма'] + [''] * (len(col_user_show_sum)-1)] + [[date] + [''] * (len(col_user_show)-1) for date in dates_max]
            for i in j
        ]]  + strokes
        post_to_google_sheet(strokes, aria_name)
