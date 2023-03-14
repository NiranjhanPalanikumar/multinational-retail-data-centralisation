import pandas as pd
import yaml
from yaml.loader import SafeLoader
from sqlalchemy import create_engine, MetaData, inspect, text

from database_utils import DatabaseConnector
from data_extraction import DataExtractor


class DataCleaning:

    @staticmethod
    def clean_user_data(users_df):
        #Checking NULL values
        print("Checking for NULL values:")
        print("--------------------------")
        for col in users_df.columns:
            indices = users_df.loc[users_df[col] == "NULL"].index.tolist()

            if len(indices) != 0:
                for index in indices:
                    users_df = users_df.drop(index)
                    #print(f"Dropping row: {index+1} of column: {col}")
                    
                print(f"Number of rows dropped for NULL in column '{col}' : {len(indices)}")
            
            else:
                print(f"No null values present in column '{col}'")
            
        print("\n")
        
        #Checking errors in dates
        for col_name in ['date_of_birth', 'join_date']:
            print(f"Checking for errors in {col_name} column:")
            print("-----------------------------------------------")
            df_date_format = pd.to_datetime(users_df[col_name], errors='coerce')
            df_date_format_series = pd.Series(df_date_format.isna())
            
            indices = df_date_format_series[df_date_format_series].index.tolist()
            indices_index = users_df.loc[indices, 'index'].tolist()
            
            for index_value in indices_index:
                users_df = users_df.drop(users_df.loc[users_df['index'] == index_value].index)

            print(f"Number of rows dropped for invlaid dates : {len(indices_index)}")

            print("\n")
            
        #Checking Numeric values in str columns 
        print("Checking for invalid numeric values:")
        print("--------------------------------------")
        
        for col in ['first_name', 'last_name', 'company', 'email_address', 'address', 'country', 'country_code', 'user_uuid']:
            if users_df[col].dtype.kind in 'biufc' == True:
                print(f"There are numeric data in column: '{col}'")

            else:
                print(f"No numeric data in column: '{col}'")

        print("\n")

        #Checking Numeric values in str columns 
        print("Checking for invalid email address:")
        print("------------------------------------")

        if users_df['email_address'].str.contains('@').sum() == users_df.shape[0]:
            print("All email address entires conatin '@' in them")
        else:
            print("Some emails are invalid which do not have '@'")

        print("\n")

        return users_df
    
    @staticmethod
    def clean_card_data(concat_df):
        #Checking NULL values
        print("Checking for NULL values:")
        print("--------------------------")

        for col in concat_df.columns:
            indices = concat_df.loc[concat_df[col] == "NULL"].index.tolist()

            if len(indices) != 0:
                for index in indices:
                    concat_df = concat_df.drop(index)
                    #print(f"Dropping row: {index+1} of column: {col}")
                    
                print(f"Number of rows dropped for NULL in column '{col}' : {len(indices)}")
            
            else:
                print(f"No null values present in column '{col}'")
            
        print("\n")

        #Checking errors in dates
        print("Checking for invalid dates:")
        print("----------------------------")

        for col_name in ['expiry_date', 'date_payment_confirmed']:

            if col_name == 'expiry_date':
                df_date_format = pd.to_datetime(concat_df[col_name], format='%m/%y', errors='coerce')
                df_date_format_series = pd.Series(df_date_format.isna())
                
                indices = df_date_format_series[df_date_format_series].index.tolist()
                indices_card_num = concat_df.loc[indices, 'card_number'].tolist()

                for card_num in indices_card_num:
                    concat_df = concat_df.drop(concat_df.loc[concat_df['card_number'] == card_num].index)

                print(f"Number of rows dropped for invlaid dates in {col_name} : {len(indices_card_num)}")

            else:
                df_date_format = pd.to_datetime(concat_df[col_name], format='%Y-%m-%d', errors='coerce')
                df_date_format_series = pd.Series(df_date_format.isna())
                
                indices = df_date_format_series[df_date_format_series].index.tolist()
                indices_card_num = concat_df.loc[indices, 'card_number'].tolist()

                for card_num in indices_card_num:
                    concat_df = concat_df.drop(concat_df.loc[concat_df['card_number'] == card_num].index)

                print(f"Number of rows dropped for invlaid dates in {col_name} : {len(indices_card_num)}")
                
            print("\n")

        return concat_df
    
    

#Testing the code
'''
db_connc = DatabaseConnector()
table_list = db_connc.list_db_tables()
#print(table_list)
table_name = table_list[1] #table: 'legacy_users'
#print(table_name)

db_extr = DataExtractor()
users_df = db_extr.read_rds_table(db_connc, table_name)

data_clean = DataCleaning()
users_df = data_clean.clean_user_data(users_df)

'''