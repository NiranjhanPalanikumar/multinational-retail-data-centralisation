import yaml
from yaml.loader import SafeLoader

class DatabaseConnector:
    
    def read_db_creds():
        file_path = "db_creds.yaml"

        with open(file_path) as f:
            cred_dict = yaml.load(f, loader=SafeLoader)

        return cred_dict
