from airflow.utils.db import provide_session
from airflow.models import Connection

def create_db_connection(ds, **kwargs):
        """"Add a db connection"""
        session = settings.Session()

        new_conn = Connection(conn_id='slack_token')
        new_conn.set_password(SLACK_LEGACY_TOKEN)

        if not (session.query(Connection).filter(Connection.conn_id == 
         new_conn.conn_id).first()):
        session.add(new_conn)
        session.commit()
        else:
            msg = '\n\tA connection with `conn_id`={conn_id} already exists\n'
            msg = msg.format(conn_id=new_conn.conn_id)
            print(msg)
    
creds = {"user": myservice.get_user(), "pwd": myservice.get_pwd() 

    c = Connection(conn_id=f'your_airflow_connection_id_here',
                   login=creds["user"],
                   host=None)
    c.set_password(creds["pwd"])
    merge_conn(c)
    

def create_conn(username, password, host=None):
    conn = Connection(conn_id=f'{username}_connection',
                                  login=username,
                                  host=host if host else None)
    conn.set_password(password)
    
            
dag = DAG(
        'create_connection',
        default_args=default_args,
        schedule_interval="@once")

t2 = PythonOperator(
        dag=dag,
        task_id='create_connection_ts',
        python_callable=create_db_connection,
        provide_context=True,
    )
