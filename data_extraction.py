import pandas as pd
import yaml
import requests
import warnings
import boto3
from io import BytesIO
from yaml.loader import SafeLoader
from sqlalchemy import create_engine, MetaData, inspect, text

import tabula

from database_utils import DatabaseConnector

class DataExtractor:
    
    @staticmethod
    def read_rds_table(db_connc, table_name):
        db_engine = db_connc.init_db_engine()
        query = f'SELECT * FROM {table_name}'
        
        with db_engine.begin() as conn:
            users_df = pd.read_sql_query(sql=text(query), con=conn)

        return users_df


    @staticmethod
    def retrieve_pdf_data(pdf_path):
        dfs = tabula.read_pdf(pdf_path, stream=True, pages='all')
        concat_df = pd.concat(dfs, ignore_index=True)

        return concat_df
    

    @staticmethod
    def list_number_of_stores(endpoint_path, header_details):
        num_stores_req = requests.get(endpoint_path, headers=header_details)
        num_stores_dict = num_stores_req.json()

        print(f"Status: {num_stores_dict['statusCode']}")
        #print(f"Number of Stores: {num_stores_dict['number_stores']}")

        return num_stores_dict['number_stores']


    @staticmethod
    def retrieve_stores_data(retrieve_store_endpoint):
        warnings.simplefilter(action='ignore', category=FutureWarning)

        for store_number in range(451):
            store_data = requests.get(retrieve_store_endpoint+str(store_number), headers={'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'})
            if store_number == 0:
                stores_df = pd.DataFrame(store_data.json(), index=[0])

            else:
                stores_df = stores_df.append(pd.DataFrame(store_data.json(), index=[0]), ignore_index=True)
                

        return stores_df
    

    @staticmethod
    def extract_from_s3(url_endpoint):
        s3 = boto3.client('s3')
        
        bucket_name, key = url_endpoint.split('://')[1].split('/',1)
        response = s3.get_object(Bucket=bucket_name, Key=key)
        content = response['Body'].read()
        csv_file = BytesIO(content)

        products_df = pd.read_csv(csv_file)

        return products_df


