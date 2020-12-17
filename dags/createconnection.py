from typing import Optional

from airflow.models import Connection, Variable
from airflow.settings import Session
from airflow.utils.db import provide_session


@provide_session
def create_connection(conn_id: str,
                      conn_type: str,
                      host: str,
                      port: Optional[int] = None,
                      user: Optional[str] = None,
                      password: Optional[str] = None,
                      schema: Optional[str] = None,
                      extra: Optional[str] = None,
                      session: Optional[Session] = None) -> None:
    conn: Connection = Connection(conn_id=conn_id,
                                  conn_type=conn_type,
                                  host=host,
                                  port=port,
                                  login=user,
                                  password=password,
                                  schema=schema,
                                  extra=extra)
    session.add(conn)
    session.flush()


@provide_session
def delete_connection(conn_id: str,
                      session: Optional[Session] = None) -> None:
    conn: Connection = session \
        .query(Connection) \
        .filter(Connection.conn_id == conn_id) \
        .one()
    session.delete(conn)
    session.flush()


def create_variable(key: str,
                    value: str,
                    serialize_json: bool = False) -> None:
    Variable.set(key=key,
                 value=value,
                 serialize_json=serialize_json)


@provide_session
def delete_variable(key: str, session: Optional[Session] = None) -> None:
    var: Variable = session \
        .query(Variable) \
        .filter(Variable.key == key) \
        .one()
    session.delete(var)
    session.flush()
