from typing import Optional
from dataclasses import dataclass
from datetime import datetime, timedelta, time
import time as ttime


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
        task_numbers = {
            ('Глубокорыхление', 'John Deere', 'GaspardoARTIGLIO4м'): 1.1,
            ('Глубокорыхление', 'Versatile', 'GaspardoARTIGLIO4м'): 1.2,
            ('Глубокорыхление', 'John Deere', 'GaspardoARTIGLIO5мскатком'): 1.3,
            ('Глубокорыхление', 'Versatile', 'GaspardoARTIGLIO5мскатком'): 1.4,
            ('глубокорыхление с внесением удобрений', 'John Deere', 'GaspardoARTIGLIO4м'): 1.1,
            ('глубокорыхление с внесением удобрений', 'Versatile', 'GaspardoARTIGLIO4м'): 1.2,
            ('глубокорыхление с внесением удобрений', 'John Deere', 'GaspardoARTIGLIO5мскатком'): 1.3,
            ('глубокорыхление с внесением удобрений', 'Versatile', 'GaspardoARTIGLIO5мскатком'): 1.4,
            ('Боронование', 'John Deere', 'пружинаяVELESБТ-!'): 2.1,
            ('Боронование', 'Кировец', 'пружинаяVELESБТ-!'): 2.2,
            ('Боронование', 'Versatile', 'пружинаяVELESБТ-!'): 2.3,
            ('Боронование', 'John Deere', 'СБГ22,2'): 2.4,
            ('Боронование', 'Кировец', 'СБГ22,2'): 2.5,
            ('Боронование'	'Versatile', 'СБГ22,2'): 2.6,
            ('Дискование', 'John Deere', 'Catros9001-KR'): 4.1,
            ('Дискование', 'Versatile', 'Catros9001-KR'): 4.2,
            ('Культивация',	'John Deere', 'GreatPlainsмодель8336FCF'): 5.1,
            ('Культивация',	'Versatile', 'GreatPlainsмодель8336FCF'): 5.2,
            ('Культивация', 'Кировец', 'GreatPlainsмодель8336FCF'): 5.3,
            ('Культивация',	'John Deere', 'КСС-11200Олимп'): 5.4,
            ('Культивация',	'Versatile', 'КСС-11200Олимп'): 5.5,
            ('Опрыскивание', 'Туман'): 8.1,
            ('Разбрасывание', 'Туман'): 9.1,
            ('Разбрасывание', 'Туман'): 9.2,
            ('Уборка', 'КСУ-1', 'Жатка9м'): 11.1,
            ('Погрузочные работы', 'JCB'): 12.1,
            ('Погрузочные работы', 'MasseyFerguson'): 12.2,
            ('Погрузочные работы', 'КамАЗ'): 12.3,
            ('Погрузочные работы', 'МТЗ'): 12.2,
            ('Посев зерновых культур', 'John Deere', 'PrimeraDMC9000RSDM68'): 6.1,
            ('Посев зерновых культур', 'Versatile', 'PrimeraDMC9000RSDM68'): 6.2,
            ('Посев зерновых культур', 'John Deere', 'JohnDeere1890'): 6.3,
            ('Посев зерновых культур', 'Versatile', 'JohnDeere1890'): 6.4,
            ('Посев зерновых культур', 'John Deere', 'GreantPlainsNTA-3510'): 6.5,
            ('Посев зерновых культур', 'Versatile', 'GreantPlainsNTA-3510'): 6.6,
            ('Посев подсолнечника', 'John Deere', 'GaspardoпропашнаяMetro16ряда'): 7.1,
            ('Посев подсолнечника', 'Versatile', 'GaspardoпропашнаяMetro16ряда'): 7.2,
            ('Посев подсолнечника', 'John Deere', 'GaspardoпропашнаяMetro24ряда'): 7.3,
            ('Посев подсолнечника', 'Versatile', 'GaspardoпропашнаяMetro24ряда'): 7.4,
            ('Посев подсолнечника', 'John Deere', 'GreantPlainsNTA-3510'): 7.5,
            ('Посев подсолнечника', 'Versatile', 'GreantPlainsNTA-3510'): 7.6
        }
        driving_numbers = {
            ('John Deere'): 13.1,
            ('John Deere', ''): 13.2,
            ('Туман'): 13.3,
            ('JCB'): 13.4,
            ('КСУ-1'): 13.5,
            ('Versatile'): 13.6,
            ('Versatile', ''): 13.7,
            ('Кировец'): 13.8,
            ('Кировец', ''): 13.9,
            ('MasseyFerguson'): 13.14,
            ('MasseyFerguson', ''): 13.15,
            ('МТЗ'): 13.16,
            ('МТЗ', ''): 13.17,
        }
        cost_ga = {
            1.1: 61.65,
            1.2: 61.65,
            1.3: 55.04,
            1.4: 55.04,
            2.1: 18.13,
            2.2: 22.02,
            2.3: 18.13,
            2.4: 19.27,
            2.5: 22.02,
            2.6: 19.27,
            4.1: 25.69,
            4.2: 25.69,
            5.1: 22.34,
            5.2: 22.34,
            5.3: 29.08,
            5.4: 22.34,
            5.5: 22.34,
            6.1: 40.59,
            6.2: 40.59,
            6.3: 33.36,
            6.4: 33.36,
            6.5: 33.36,
            6.6: 33.36,
            7.1: 32.47,
            7.2: 32.47,
            7.3: 32.47,
            7.4: 32.47,
            7.5: 33.36,
            7.6: 33.36,
            8.1: 14.76,
            9.1: 12.82,
            9.2: 17.35,
            11.1: 34.78,
            12.1: 113.17,
            12.2: 113.17,
            12.3: 113.17,
            12.2: 113.17
        }
        # км
        cost_km = {
            13.1: 2.10,
            13.2: 3.15,
            13.3: 2.10,
            13.4: 2.10,
            13.5: 2.10,
            13.6: 2.10,
            13.7: 3.15,
            13.8: 2.10,
            13.9: 3.15,
            13.10: 2.63,
            13.11: 2.63,
            13.12: 2.63,
            13.13: 2.63,
            13.14: 2.10,
            13.15: 3.15,
            13.16: 2.10,
            13.17: 3.15
        }
        implement = implement_name.replace('"', '').replace(' ', '')
        task_key = (work_type, machine_manufacturer, implement)
        result = 0
        result_driving = 0
        if task_key in task_numbers or work_type == 'Перегон':
            if work_type != 'Перегон':
                result = cost_ga[task_numbers[task_key]] * self.covered_area
            else:
                result = ''
            distance = self.total_distance - self.work_distance
            distance = distance / 1000 if distance >= 10_000 else 0
            if distance:
                driving_key = (machine_manufacturer, '') if self.implement_id else (machine_manufacturer)
                if driving_key in driving_numbers:
                    result_driving = driving_numbers[driving_key] * distance
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
                  "Выработка (га)": self.covered_area,
                  "Выработка (км)": distance / 1000 if distance >= 10_000 else 0,
                  "Работа (руб.)": cost[0],
                  "Перегон (руб.)": cost[1],
                  "Водитель (руб.)": driver}
        return result