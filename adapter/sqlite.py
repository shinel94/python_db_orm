from .base import BaseAdapter
import sqlite3
from .. import constant as const


class SqliteAdapter(BaseAdapter):
    db_path = const.G_SQLITE_INFO['db_path']

    @classmethod
    def select(cls, a_table_name, a_invert_schema, a_select_parameter):
        col_list = ', '.join(list(a_invert_schema.values()))
        where_query = []
        values = []
        for col_name, col_param in a_select_parameter.items():
            if col_param is not None:
                where_query.append(f"{a_invert_schema[col_name]}=?")
                values.append(col_param)
        if len(where_query) == 0:
            raise ValueError("전체 선택은 지원하지 않습니다.")
        where_query = ' AND '.join(where_query)
        query = f"SELECT {col_list} FROM {a_table_name} WHERE {where_query} ORDER BY {a_table_name}_id DESC"

        with sqlite3.connect(cls.db_path, timeout=const.G_DB_TIMEOUT_SEC) as db:
            cursor = db.cursor()
            cursor.execute(query, values)
            data = cursor.fetchall()
            db.commit()
        return data

    @classmethod
    def select_or(cls, a_table_name, a_invert_schema, a_select_parameter):
        col_list = ', '.join(list(a_invert_schema.values()))
        where_query = []
        values = []
        for col_name, col_param in a_select_parameter.items():
            if col_param is not None:
                where_query.append(f"{a_invert_schema[col_name]}=?")
                values.append(col_param)
        if len(where_query) == 0:
            raise ValueError("전체 선택은 지원하지 않습니다.")
        where_query = ' OR '.join(where_query)
        query = f"SELECT {col_list} FROM {a_table_name} WHERE {where_query} ORDER BY {a_table_name}_id DESC"

        with sqlite3.connect(cls.db_path, timeout=const.G_DB_TIMEOUT_SEC) as db:
            cursor = db.cursor()
            cursor.execute(query, values)
            data = cursor.fetchall()
            db.commit()
        return data

    @classmethod
    def insert(cls, a_table_name, a_schema, a_field_values):

        question = ','.join(['?'] * len(a_schema))
        query = f"INSERT INTO {a_table_name}({','.join(a_schema.keys())}) VALUES({question})"
        print(query)
        with sqlite3.connect(cls.db_path, timeout=const.G_DB_TIMEOUT_SEC) as db:
            cursor = db.cursor()
            cursor.execute(query, a_field_values)
            db.commit()

    @classmethod
    def update(cls, a_record_index, a_table_name, a_invert_schema, a_select_parameter):
        set_query = []
        values = []
        for col_name, col_param in a_select_parameter.items():
            if col_param is not None:
                set_query.append(f"{a_invert_schema[col_name]} = ?")
                values.append(col_param)
        if len(set_query) == 0:
            raise ValueError("변화된 프로퍼티가 존재하지 않습니다.")
        set_query = ', '.join(set_query)
        query = f"UPDATE {a_table_name} SET {set_query} WHERE {a_table_name.lower()}_id = {a_record_index}"

        with sqlite3.connect(cls.db_path, timeout=const.G_DB_TIMEOUT_SEC) as db:
            cursor = db.cursor()
            cursor.execute(query, values)
            db.commit()


    @classmethod
    def get_index(cls, a_table_name):
        query = f"SELECT SEQ FROM SQLITE_SEQUENCE WHERE NAME=?"
        with sqlite3.connect(cls.db_path, timeout=const.G_DB_TIMEOUT_SEC) as db:
            cursor = db.cursor()
            cursor.execute(query, (a_table_name, ))
            data = cursor.fetchall()
            db.commit()
        if len(data) == 0:
            return 0
        else:
            return int(data[0][0]) + 1

    @classmethod
    def select_n(cls, a_table_name, a_invert_schema, a_select_parameter, a_num_record):
        col_list = ', '.join(list(a_invert_schema.values()))
        where_query = []
        values = []
        for col_name, col_param in a_select_parameter.items():
            if col_param is not None:
                where_query.append(f"{a_invert_schema[col_name]}=?")
                values.append(col_param)
        if len(where_query) == 0:
            raise ValueError("전체 선택은 지원하지 않습니다.")
        where_query = ' AND '.join(where_query)
        query = f"SELECT {col_list} FROM {a_table_name} WHERE {where_query} ORDER BY {a_table_name}_id DESC LIMIT {a_num_record}"

        with sqlite3.connect(cls.db_path, timeout=const.G_DB_TIMEOUT_SEC) as db:
            cursor = db.cursor()
            cursor.execute(query, values)
            data = cursor.fetchall()
            db.commit()
        return data

    @classmethod
    def delete(cls, a_table_name, a_invert_schema, a_select_parameter):
        col_list = ', '.join(list(a_invert_schema.values()))
        where_query = []
        values = []
        for col_name, col_param in a_select_parameter.items():
            if col_param is not None:
                where_query.append(f"{a_invert_schema[col_name]}=?")
                values.append(col_param)
        if len(where_query) == 0:
            raise ValueError("전체 삭제는 지원하지 않습니다.")
        where_query = ' AND '.join(where_query)
        query = f"DELETE FROM {a_table_name} WHERE {where_query}"

        with sqlite3.connect(cls.db_path, timeout=const.G_DB_TIMEOUT_SEC) as db:
            cursor = db.cursor()
            cursor.execute(query, values)
            db.commit()
        return True

