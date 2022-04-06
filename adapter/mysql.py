from .base import BaseAdapter
import mysql.connector
from .. import constant as const


class MysqlAdapter(BaseAdapter):
    db_host = const.G_MYSQL_INFO['host']
    db_user = const.G_MYSQL_INFO['user']
    db_pwd = const.G_MYSQL_INFO['pwd']
    db_name = const.G_MYSQL_INFO['name']
    db_port = const.G_MYSQL_INFO['port']
    db_timeout = const.G_MYSQL_INFO['timeout']

    @classmethod
    def get_connect_args(cls):
        s_connect_arge = {
            "host": cls.db_host,
            "port": cls.db_port,
            "user": cls.db_user,
            "password": cls.db_pwd,
            "database": cls.db_name,
        }
        if cls.db_timeout is not None:
            s_connect_arge['connection_timeout'] = cls.db_timeout
        return s_connect_arge

    @classmethod
    def select(cls, a_table_name, a_invert_schema, a_select_parameter):
        col_list = ', '.join(list(a_invert_schema.values()))
        where_query = []
        values = []
        for col_name, col_param in a_select_parameter.items():
            if col_param is not None:
                where_query.append(f"{a_invert_schema[col_name]}=%s")
                values.append(col_param)
        if len(where_query) == 0:
            raise ValueError("전체 선택은 지원하지 않습니다.")
        where_query = ' AND '.join(where_query)
        query = f"SELECT {col_list} FROM {a_table_name} WHERE {where_query} ORDER BY {a_table_name}_id DESC"
        db = mysql.connector.connect(**cls.get_connect_args())
        try:
            cursor = db.cursor()
            cursor.execute(query, values)
            data = cursor.fetchall()
            db.commit()
        finally:
            db.close()
        return data

    @classmethod
    def select_or(cls, a_table_name, a_invert_schema, a_select_parameter):
        col_list = ', '.join(list(a_invert_schema.values()))
        where_query = []
        values = []
        for col_name, col_param in a_select_parameter.items():
            if col_param is not None:
                where_query.append(f"{a_invert_schema[col_name]}=%s")
                values.append(col_param)
        if len(where_query) == 0:
            raise ValueError("전체 선택은 지원하지 않습니다.")
        where_query = ' OR '.join(where_query)
        query = f"SELECT {col_list} FROM {a_table_name} WHERE {where_query} ORDER BY {a_table_name}_id DESC"
        db = mysql.connector.connect(**cls.get_connect_args())
        try:
            cursor = db.cursor()
            cursor.execute(query, values)
            data = cursor.fetchall()
            db.commit()
        finally:
            db.close()
        return data

    @classmethod
    def insert(cls, a_table_name, a_schema, a_field_values):

        question = ','.join(['%s'] * len(a_schema))
        query = f"INSERT INTO {a_table_name}({','.join(a_schema.keys())}) VALUES({question})"
        db = mysql.connector.connect(**cls.get_connect_args())
        try:
            cursor = db.cursor()
            cursor.execute(query, a_field_values)
            db.commit()
        finally:
            db.close()

    @classmethod
    def update(cls, a_record_index, a_table_name, a_invert_schema, a_select_parameter):
        set_query = []
        values = []
        for col_name, col_param in a_select_parameter.items():
            if col_param is not None:
                set_query.append(f"{a_invert_schema[col_name]} = %s")
                values.append(col_param)
        if len(set_query) == 0:
            raise ValueError("변화된 프로퍼티가 존재하지 않습니다.")
        set_query = ', '.join(set_query)
        query = f"UPDATE {a_table_name} SET {set_query} WHERE {a_table_name.lower()}_id = {a_record_index}"

        db = mysql.connector.connect(**cls.get_connect_args())
        try:
            cursor = db.cursor()
            cursor.execute(query, values)
            db.commit()
        finally:
            db.close()


    @classmethod
    def get_index(cls, a_table_name):
        query = f"select AUTO_INCREMENT from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA=%s AND TABLE_NAME=%s"
        db = mysql.connector.connect(**cls.get_connect_args())
        try:
            cursor = db.cursor()
            cursor.execute(query, (cls.db_name, a_table_name, ))
            data = cursor.fetchall()
            db.commit()
        finally:
            db.close()

        if data[0][0] is None:
            return 1
        else:
            return int(data[0][0])

    @classmethod
    def select_n(cls, a_table_name, a_invert_schema, a_select_parameter, a_num_record):
        col_list = ', '.join(list(a_invert_schema.values()))
        where_query = []
        values = []
        for col_name, col_param in a_select_parameter.items():
            if col_param is not None:
                where_query.append(f"{a_invert_schema[col_name]}=%s")
                values.append(col_param)
        if len(where_query) == 0:
            raise ValueError("전체 선택은 지원하지 않습니다.")
        where_query = ' AND '.join(where_query)
        query = f"SELECT {col_list} FROM {a_table_name} WHERE {where_query} ORDER BY {a_table_name}_id DESC LIMIT {a_num_record}"
        db = mysql.connector.connect(**cls.get_connect_args())
        try:
            cursor = db.cursor()
            cursor.execute(query, values)
            data = cursor.fetchall()
            db.commit()
        finally:
            db.close()
        return data

    @classmethod
    def delete(cls, a_table_name, a_invert_schema, a_select_parameter):
        col_list = ', '.join(list(a_invert_schema.values()))
        where_query = []
        values = []
        for col_name, col_param in a_select_parameter.items():
            if col_param is not None:
                where_query.append(f"{a_invert_schema[col_name]}=%s")
                values.append(col_param)
        if len(where_query) == 0:
            raise ValueError("전체 삭제는 지원하지 않습니다.")
        where_query = ' AND '.join(where_query)
        query = f"DELETE FROM {a_table_name} WHERE {where_query}"
        db = mysql.connector.connect(**cls.get_connect_args())
        try:
            cursor = db.cursor()
            cursor.execute(query, values)
            db.commit()
        finally:
            db.close()
        return True
