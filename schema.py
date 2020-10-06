from marshmallow import Schema, fields, EXCLUDE, post_load, validate

from data import Driver, Task, TaskFieldMapping, Machine, Implement, WorkTypeGroup, WorkType, Field


class BaseSchema(Schema):
    id = fields.Integer()

    class Meta:
        unknown = EXCLUDE


class FieldSchema(BaseSchema):
    name = fields.Str()

    @post_load
    def make_field(self, data, **kwargs):
        return Field(**data)


class DriverSchema(BaseSchema):
    username = fields.Str()
    updated_at = fields.DateTime()

    @post_load
    def make_driver(self, data, **kwargs):
        return Driver(**data)


class MachineSchema(BaseSchema):
    name = fields.Str()
    updated_at = fields.DateTime()

    @post_load
    def make_machine(self, data, **kwargs):
        return Machine(**data)


class ImplementSchema(BaseSchema):
    name = fields.Str()
    updated_at = fields.DateTime()

    @post_load
    def make_implement(self, data, **kwargs):
        return Implement(**data)


class WorkTypeGroupSchema(BaseSchema):
    name = fields.Str()
    updated_at = fields.DateTime()

    @post_load
    def make_work_type_groups(self, data, **kwargs):
        return WorkTypeGroup(**data)


class WorkTypeSchema(BaseSchema):
    work_type_group_id = fields.Integer(required=True)
    name = fields.Str(required=True)
    updated_at = fields.DateTime()

    @post_load
    def make_work_types(self, data, **kwargs):
        return WorkType(**data)


class TaskFieldMappingSchema(BaseSchema):
    machine_task_id = fields.Integer()
    field_id = fields.Integer()
    covered_area = fields.Float()
    updated_at = fields.DateTime()

    @post_load
    def make_task_field_mapping(self, data, **kwargs):
        return TaskFieldMapping(**data)


class TaskSchema(BaseSchema):
    machine_id = fields.Integer()
    implement_id = fields.Integer(missing=None)
    driver_id = fields.Integer(missing=None)
    work_type_id = fields.Integer()
    start_time = fields.DateTime()
    end_time = fields.DateTime()
    updated_at = fields.DateTime()
    fuel_consumption = fields.Float()
    covered_area = fields.Float()
    total_distance = fields.Float()
    work_distance = fields.Float()

    @post_load
    def make_task(self, data, **kwargs):
        return Task(**data)
