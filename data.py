from typing import Optional
from dataclasses import dataclass
from datetime import datetime, timedelta, time


@dataclass
class Field():
    id: int
    name: str


@dataclass
class Driver():
    id: int
    username: str
    updated_at: datetime


@dataclass
class Machine():
    id: int
    name: str
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
    driver_id: Optional[int] = None
    implement_id: Optional[int] = None

    def dict_for_user(self, machine_name, implement_name, fields, work_msg):
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

        result = {"Дата": self.start_time.date().strftime(r"%Y-%m-%d"),
                  "Дневная смена": day_shift,
                  "Ночная смена": night_shift,
                  "Техника": machine_name,
                  "Агрегат": implement_name,
                  "Номер полей": fields,
                  "Операция": work_msg,
                  "Расход топлива": self.fuel_consumption,
                  "Выработка (га)": self.covered_area,
                  "Выработка (км)": distance / 1000 if distance >= 10_000 else 0}
        return result


@dataclass
class TaskForUser():
    id: int
    machine: int
    start_time: datetime
    end_time: datetime
    fuel_consumption: float
    covered_area: float
    total_distance: float
    work_distance: float
    driver: Optional[int] = None
    implement: Optional[int] = None
