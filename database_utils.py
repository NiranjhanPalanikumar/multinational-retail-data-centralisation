import yaml
from yaml.loader import SafeLoader
from sqlalchemy import create_engine, MetaData, inspect

class DatabaseConnector:

    def __init__(self, file_path="E:\Studies\AiCore\Project_4_Multinational_Data\Project_Files\multinational-retail-data-centralisation\db_creds.yaml"):
        self.file_path = file_path
    

    def read_db_creds(self):
        with open(self.file_path) as f:
            db_cred_dict = yaml.load(f, loader=SafeLoader)

        return db_cred_dict


    def init_db_engine(self):
        #getting the dictionary credentials
        db_cred_dict = self.read_db_creds()

        #forming the database URL
        db_url = f'postgresql://{db_cred_dict["RDS_USER"]}:{db_cred_dict["RDS_PASSWORD"]}@{db_cred_dict["RDS_HOST"]}:{db_cred_dict["RDS_PORT"]}/{db_cred_dict["RDS_DATABASE"]}'

        #creating the engine
        db_engine = create_engine(db_url)
        return db_engine
    

    def list_db_tables(self):
        db_engine = self.init_db_engine()
        db_insp = inspect(db_engine)
        db_table_list = db_insp.get_table_names()

        return db_table_list