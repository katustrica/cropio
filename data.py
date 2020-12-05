import configparser
from pathlib import Path
from typing import Optional
from dataclasses import dataclass
from datetime import datetime, timedelta, time
import time as ttime
import pandas as pd


config = configparser.ConfigParser()
config.read('config.ini')

settings_path = Path(config['settings']['path'])
prices_excel = settings_path / Path(config['settings']['prices_excel'])


@dataclass
class Field():
    id: int
    name: str
    updated_at: datetime


@dataclass
class Driver():
    id: int
    username: str
    updated_at: datetime
    driver: bool


@dataclass
class Machine():
    id: int
    name: str
    manufacturer: str
    updated_at: datetime


@dataclass
class Implement():
    id: int
    name: str
    updated_at: datetime


@dataclass
class WorkTypeGroup():
    id: int
    name: str
    updated_at: datetime


@dataclass
class WorkType():
    id: int
    work_type_group_id: int
    updated_at: datetime
    name: str


@dataclass
class TaskFieldMapping():
    id: int
    machine_task_id: int
    field_id: int
    covered_area: float
    updated_at: datetime


@dataclass
class Task():
    id: int
    machine_id: int
    start_time: datetime
    end_time: datetime
    updated_at: datetime
    fuel_consumption: float
    covered_area: float
    total_distance: float
    work_distance: float
    work_type_id: int
    stops_on_road_duration: int
    driver_id: Optional[int] = None
    implement_id: Optional[int] = None

    def calculate_cost(self, machine_manufacturer, implement_name, work_type):
        implement = implement_name.replace('"', '').replace(' ', '')
        task_key = (work_type, machine_manufacturer, implement)
        result = 0
        result_driving = 0
        prices = pd.read_excel(prices_excel)
        task_df = (
            prices.loc[(prices['Имя операции'] == work_type)
                       & (prices['Машина'] == machine_manufacturer)
                       & (prices['Оборудование'] == implement)]
        )
        driving_implement = 'Агрегат' if self.implement_id else 'Нетагрегата'
        driving_df = (
            prices.loc[(prices['Имя машины перегон'] == machine_manufacturer)
                       & (prices['Оборудование'] == driving_implement)]
        )

        if not task_df.empty or work_type == 'Перегон':
            if work_type != 'Перегон':
                result = round(float(task_df['Стоимость за гектар']) * self.covered_area, 2)
            else:
                result = ''
            distance = round(self.total_distance - self.work_distance, 2)
            distance = distance / 1000 if distance >= 10_000 else 0
            if distance:
                if not driving_df.empty:
                    result_driving = round(float(driving_df['Стоимость за км']) * distance, 2)
                else:
                    result_driving = 'Невозможно посчитать'
        else:
            result = "Невозможно посчитать"
        return result, result_driving

    def dict_for_user(self,
                      machine_name,
                      machine_manufacturer,
                      work_type,
                      implement_name,
                      fields,
                      work_msg,
                      driver):
        distance = self.total_distance - self.work_distance
        night_shift = 0
        day_shift = 0
        start_time = self.start_time

        day = time(6, 00)
        night = time(22, 00)

        while (start_time + timedelta(hours=1)) < self.end_time:
            if night > start_time.time() > day:
                day_shift += 1
            else:
                night_shift += 1
            start_time += timedelta(hours=1)
        cost = self.calculate_cost(machine_manufacturer, implement_name, work_type)
        result = {"Дата": self.start_time.date().strftime(r"%Y-%m-%d"),
                  "Дневная смена": day_shift,
                  "Ночная смена": night_shift,
                  "Техника": machine_name,
                  "Агрегат": implement_name,
                  "Номер полей": fields,
                  "Операция": work_msg,
                  "Расход топлива": self.fuel_consumption,
                  "Простой": ttime.strftime('%H:%M:%S', ttime.gmtime(self.stops_on_road_duration)),
                  "Выработка (га)": round(self.covered_area, 2),
                  "Выработка (км)": round(distance / 1000, 2) if distance >= 10_000 else 0,
                  "Работа (руб.)": cost[0],
                  "Перегон (руб.)": cost[1],
                  "Водитель (руб.)": driver}
        return result