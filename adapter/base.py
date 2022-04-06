from abc import ABCMeta, abstractmethod


class BaseAdapter(metaclass=ABCMeta):

    @classmethod
    @abstractmethod
    def select(cls, a_table_name, a_invert_schema, a_select_parameter):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def select_or(cls, a_table_name, a_invert_schema, a_select_parameter):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def insert(cls, a_table_name, a_schema, a_field_values):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def update(cls, a_record_index, a_table_name, a_invert_schema, a_select_parameter):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def delete(cls, a_table_name, a_invert_schema, a_select_parameter):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get_index(cls, a_table_name):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def select_n(cls, a_table_name, a_invert_schema, a_select_parameter, a_num_record):
        return NotImplementedError