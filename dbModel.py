from abc import ABCMeta, abstractmethod
from .adapter import SqliteAdapter, MysqlAdapter, BaseAdapter
from . import constant as const
from datetime import datetime
# import constant as const


def get_adapter() -> BaseAdapter:
    if const.G_DB_CONNECTOR == 'sqlite':
        return SqliteAdapter()
    if const.G_DB_CONNECTOR == 'mysql':
        return MysqlAdapter()
    else:
        raise ValueError('Adapter Type is Not Valid')


class DataBaseModel(metaclass=ABCMeta):
    table_name: str = ""
    schema = {
        'db_field_name': 'class_member_name'
    }
    db_adapter = get_adapter()
    _invert_schema = None

    def __repr__(self):
        return str({prop_name: getattr(self, prop_name, "Empty") for prop_name in self.schema.values()})

    def __init__(self, a_id, *args, **kwargs):
        self.id = a_id

    @classmethod
    def invert_schema(cls):
        if cls._invert_schema is None:
            cls._invert_schema = {member_name: field_name for field_name, member_name in cls.schema.items()}
        return cls._invert_schema

    @classmethod
    def from_row(cls, a_row):
        a_row = [col if not isinstance(col, datetime) else col.strftime(const.G_DATETIME_STRING_FORMAT) for col in a_row]
        return cls(*a_row)

    @classmethod
    def from_row_except_column(cls, a_row, a_col_idx):
        a_row = [*a_row[:a_col_idx], None, *a_row[a_col_idx:]]
        return cls.from_row(a_row)

    @classmethod
    def _select(cls, **kwargs):
        return cls.db_adapter.select(cls.table_name, cls.invert_schema(), kwargs)

    @classmethod
    def _select_or(cls, **kwargs):
        return cls.db_adapter.select_or(cls.table_name, cls.invert_schema(), kwargs)

    @classmethod
    def _select_n(cls, a_record_number, **kwargs):
        return cls.db_adapter.select_n(cls.table_name, cls.invert_schema(), kwargs, a_record_number)

    def insert(self):
        values = tuple(getattr(self, prop_name) for prop_name in self.schema.values())
        self.db_adapter.insert(self.table_name, self.schema, values)

    def _update(self, **kwargs):
        try:
            self.db_adapter.update(self.id, self.table_name, self.invert_schema(), kwargs)
        except ValueError:
            # 변경된 변수가 없어서 본래는 ValueError가 발생하나 Catch하여 로직은 이상이 없도록 진행
            pass
        self.update_property()

    @abstractmethod
    def update_property(self):
        raise NotImplementedError

    def update(self):
        update_parameter_dict = {}
        for prop_name in self.schema.values():
            try:
                if getattr(self, prop_name) != getattr(self, '_prev_' + prop_name):
                    update_parameter_dict[prop_name] = getattr(self, prop_name)
            except AttributeError:
                pass
        return self._update(**update_parameter_dict)

    @classmethod
    @abstractmethod
    def select(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def select_or(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def get_index(cls):
        return cls.db_adapter.get_index(cls.table_name)

    @classmethod
    @abstractmethod
    def select_n(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def delete(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def _delete(cls, **kwargs):
        try:
            return cls.db_adapter.delete(cls.table_name, cls.invert_schema(), kwargs)
        except ValueError:
            return False


class Sample(DataBaseModel):
    table_name = "sample"
    schema = {
        'sample_id': 'id',
        'col_1': 'prop_1',
        'col_2': 'prop_2',
        'col_3': 'prop_3',
    }  # table_column_name : property, 반드시 id의 경우 {table_name}_id 여야 한다.


    def __init__(self, a_id, a_arg_1, a_arg_2, a_arg_3,):
        self.prop_1 = a_arg_1
        self.prop_2 = a_arg_2
        self.prop_3 = a_arg_3

        self._prev_prop_1 = self.prop_1
        self._prev_prop_2 = self.prop_2
        self._prev_prop_3 = self.prop_3
        super().__init__(a_id)

    @classmethod
    def select(cls, id=None, prop_1=None, prop_2=None, prop_3=None):
        selected_row = cls._select(id=id, prop_1=prop_1, prop_2=prop_2, prop_3=prop_3)
        return [cls.from_row(row) for row in selected_row]

    @classmethod
    def select_or(cls, id=None, prop_1=None, prop_2=None, prop_3=None):
        selected_row = cls._select_or(id=id, prop_1=prop_1, prop_2=prop_2, prop_3=prop_3)
        return [cls.from_row(row) for row in selected_row]

    @classmethod
    def select_n(cls, a_record_number, id=None, prop_1=None, prop_2=None, prop_3=None):
        selected_row = cls._select_n(a_record_number=a_record_number, id=id, prop_1=prop_1, prop_2=prop_2, prop_3=prop_3)
        return [cls.from_row(row) for row in selected_row]

    def update_property(self):
        self._prev_prop_1 = self.prop_1
        self._prev_prop_2 = self.prop_2
        self._prev_prop_3 = self.prop_3

    @classmethod
    def delete(cls, id=None, prop_1=None, prop_2=None, prop_3=None):
        cls._delete(id=id, prop_1=prop_1, prop_2=prop_2, prop_3=prop_3)