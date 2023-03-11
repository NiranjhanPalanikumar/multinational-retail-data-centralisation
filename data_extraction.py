import pandas as pd
import yaml
from yaml.loader import SafeLoader
from sqlalchemy import create_engine, MetaData, inspect, text

from database_utils import DatabaseConnector

class DataExtractor:
    
    @staticmethod
    def read_rds_table(db_connc, table_name):
        db_engine = db_connc.init_db_engine()
        query = f'SELECT * FROM {table_name}'
        
        with db_engine.begin() as conn:
            users_df = pd.read_sql_query(sql=text(query), con=conn)

        return users_df
