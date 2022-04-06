import os

# G_DB_CONNECTOR = 'sqlite'
G_DB_CONNECTOR = 'mysql'

G_SQLITE_INFO = {
    'db_path': os.getenv("SQLITE_DB_FILE_PATH", 'sqlite3.db')
}

G_MYSQL_INFO = {
    'host': os.getenv("MYSQL_IP", '127.0.0.1'),
    'user': os.getenv("MYSQL_ID", 'root'),
    'pwd': os.getenv("MYSQL_PW", 'root'),
    'name': os.getenv("MYSQL_DB_NAME", 'db'),
    'port': int(os.getenv("MYSQL_PORT", 3306)),
    'timeout': int(os.getenv("MYSQL_CONNECTION_TIMEOUT")) if os.getenv("MYSQL_CONNECTION_TIMEOUT", None) is not None else None
}

G_DB_TIMEOUT_SEC = 5

G_DATETIME_STRING_FORMAT = "%Y-%m-%d %H:%M:%S"
