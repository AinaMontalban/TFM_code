from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine

try:
    postgres_db = {'drivername': 'postgresql+psycopg2',
                'username': 'postgres',
                'password': 'postgres',
                'host': 'localhost',
                'database': 'ngs_quality_db'}
    url_conn=URL(**postgres_db)
    engine = create_engine(url_conn)
    print("Connection succesfully!")
except:
    print("Not connection")
#    engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost/ngs_quality_db')
