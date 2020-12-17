from airflow.settings import Session
from airflow.utils.db import provide_session
from airflow.models import Pool

# hive_pool is just an example, you might want to create some other types of pools such as for MySQL
@provide_session
def create_hive_pool(session: Optional[Session] = None) -> None:
    pool = Pool(pool=pool_templates['hive_name'],
        		slots=1,
        		description=pool_templates['hive_description'])
    session.add(pool)
