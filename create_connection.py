from airflow.models import Connection
from airflow.hooks.base_hook import BaseHook

def create_conn(username, password, host=None):
    new_conn = Connection(conn_id=f'{username}_connection',
                                  login=username,
                                  host=host if host else None)
    new_conn.set_password(password)
    

connection = BaseHook.get_connection("username_connection")
password = connection.password # This is a getter that returns the unencrypted password.
