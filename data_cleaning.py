import pandas as pd
import yaml
import datetime
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

                for num in indices_card_num:
                    #print(concat_df.loc[concat_df['card_number'] == num])
                    values_split = concat_df.loc[concat_df['card_number'] == num, 'date_payment_confirmed'].iloc[0].split(' ')
                    
                    for value in values_split:
                        if len(value) == 2:
                            day_value = value
                        elif len(value) == 4 and value.isnumeric() == True:
                            year_value = value
                        else:
                            month_value = value


                    month_number = '{:02d}'.format(datetime.datetime.strptime(month_value, '%B').month)
                    final_date = year_value + '-' + str(month_number) + '-' + day_value

                    print(f"Changing {concat_df.loc[concat_df['card_number'] == num, 'date_payment_confirmed'].iloc[0]} to {final_date}")
                    concat_df.loc[concat_df['card_number'] == num, 'date_payment_confirmed'] = final_date
                
            print("\n")

        return concat_df
    

    @staticmethod
    def clean_store_data(stores_df):
        print("Checking for Non-Numeric values in numeric columns:")
        print("-----------------------------------------------------")

        for col_name in ['longitude', 'staff_numbers', 'latitude']:

            stores_df_col = pd.to_numeric(stores_df[col_name], errors='coerce')
            stores_df_col_series = pd.Series(stores_df_col.isna())

            indices = stores_df_col_series[stores_df_col_series].index.tolist()
            indices_index = stores_df.loc[indices, 'index'].tolist()

            for index_num in indices_index:
                stores_df = stores_df.drop(stores_df.loc[stores_df['index'] == index_num].index)

            print(f"Number of rows dropped for invlaid values in column {col_name}  : {len(indices_index)}")
            print('\n')

        print("Checking for Invalid Dates:")
        print("-----------------------------")

        stores_df_date_format = pd.to_datetime(stores_df['opening_date'], format='%Y-%m-%d', errors='coerce')
        stores_df_date_format_series = pd.Series(stores_df_date_format.isna())

        indices = stores_df_date_format_series[stores_df_date_format_series].index.tolist()
        indices_index = stores_df.loc[indices, 'index'].tolist()

        #print(indices_card_num)

        for index in indices_index:
            #print(stores_df.loc[stores_df['index'] == num, 'opening_date'])
            values_split = stores_df.loc[stores_df['index'] == index, 'opening_date'].iloc[0].split(' ')

            for value in values_split:
                if len(value) == 2:
                    day_value = value
                elif len(value) == 4 and value.isnumeric() == True:
                    year_value = value
                else:
                    month_value = value

            month_number = '{:02d}'.format(datetime.datetime.strptime(month_value, '%B').month)
            final_date = year_value + '-' + str(month_number) + '-' + day_value

            print(f"Changing {stores_df.loc[stores_df['index'] == index, 'opening_date'].iloc[0]} to {final_date}")
            stores_df.loc[stores_df['index'] == index, 'opening_date'] = final_date

        return stores_df


    @staticmethod
    def clean_products_data(products_df):
        print("Checking for NaN values:")
        print("-------------------------")

        for col in products_df.columns:
            print(f" Number of rows removed for NaN values in column {col}: {products_df[col].isna().sum()}")
            products_df.dropna(subset=[col], inplace=True)
        print('\n')

        print("Checking for Non-Numeric values in numeric columns:")
        print("-----------------------------------------------------")

        products_df_col = pd.to_numeric(products_df['EAN'], errors='coerce')
        products_df_col_series = pd.Series(products_df_col.isna())

        indices = products_df_col_series[products_df_col_series].index.tolist()
        indices_index = products_df.loc[indices, 'Unnamed: 0'].tolist()

        print(indices_index)

        for index_num in indices_index:
            products_df = products_df.drop(products_df.loc[products_df['Unnamed: 0'] == index_num].index)

        print(f"Number of rows dropped for invlaid values in column 'EAN'  : {len(indices_index)}")
        print('\n')

        print("Checking for Invalid Dates:")
        print("-----------------------------")

        products_df_date_format = pd.to_datetime(products_df['date_added'], format='%Y-%m-%d', errors='coerce')
        products_df_date_format_series = pd.Series(products_df_date_format.isna())

        indices = products_df_date_format_series[products_df_date_format_series].index.tolist()
        indices_index = products_df.loc[indices, 'Unnamed: 0'].tolist()

        #print(indices_index)

        for index in indices_index:
                #print(stores_df.loc[stores_df['index'] == num, 'opening_date'])
                values_split = products_df.loc[products_df['Unnamed: 0'] == index, 'date_added'].iloc[0].split(' ')

                for value in values_split:
                    if len(value) == 2:
                        day_value = value
                    elif len(value) == 4 and value.isnumeric() == True:
                        year_value = value
                    else:
                        month_value = value


                month_number = '{:02d}'.format(datetime.datetime.strptime(month_value, '%B').month)
                final_date = year_value + '-' + str(month_number) + '-' + day_value

                print(f"Changing {products_df.loc[products_df['Unnamed: 0'] == index, 'date_added'].iloc[0]} to {final_date}")
                products_df.loc[products_df['Unnamed: 0'] == index, 'date_added'] = final_date


        print('\n')
        
        return products_df
    

    @staticmethod
    def isfloat(value):
        try:
            float(value)
            return True
        except ValueError:
            return False
    

    @staticmethod
    def convert_product_weights(products_df):
        products_df = DataCleaning.clean_products_data(products_df)
        print("Checking for Weights column:")
        print("-----------------------------")

        count = 0
        for i in products_df.index.tolist():
            unit = []
            ind = -1
            #print(f"{i} -> {products_df.loc[i, 'weight']}")

            while products_df.loc[i, 'weight'][ind].isalpha() == True:
                unit = [products_df.loc[0, 'weight'][ind]] + unit
                ind -= 1

            unit = ''.join(unit)
            #print(unit)

            if DataCleaning.isfloat(products_df.loc[i, 'weight'][:(len(products_df.loc[i, 'weight'])+ind+1)]) == False:

                if " x " in products_df.loc[i, 'weight'][:(len(products_df.loc[i, 'weight'])+ind+1)]:
                    value = products_df.loc[i, 'weight'][:(len(products_df.loc[i, 'weight'])+ind+1)]    #{unit}")
                    #print(value)

                    value = value.split(' x ')
                    #print(value)

                    value_new = str((float(value[0]) * float(value[1]))/1000) + 'kg'
                    #print(f"{value} -> {value_new}")

                    products_df.loc[i, 'weight'] = value_new
                    count += 1

                else:
                    #print(f"index {i} -> {products_df.loc[i, 'weight'][:(len(products_df.loc[i, 'weight'])+ind+1)]} {unit}")
                    value = products_df.loc[i,'weight'][:2]
                    value_new = str(float(value)/1000) + 'kg'

                    #print(value_new)
                    products_df.loc[i, 'weight'] = value_new
                    count += 1

                    
            elif unit == 'kg':    
                value = float(products_df.loc[i, 'weight'][:(len(products_df.loc[i, 'weight'])+ind+1)])
                #print(f"{value} {unit}")

            elif unit == 'g' or unit == 'ml':
                value = float(products_df.loc[i, 'weight'][:(len(products_df.loc[i, 'weight'])+ind+1)])
                value_new = str(value/1000) + 'kg'
                #print(f"{value} {unit} -> {value_new} kg")

                products_df.loc[i, 'weight'] = value_new
                count += 1

            else:
                print(f"index: {i}  has unit: {unit}")

        print(f"Converted '{count}' weight rows into units of kg")

        return products_df


    @staticmethod
    def clean_orders_data(orders_df):
            
        print("Removing columns 'first_name', 'last_name', and '1':")
        print("----------------------------------------------------")

        orders_df = orders_df.drop(columns=['first_name', 'last_name', '1'])
        print("Removed column: 'first_name'")
        print("Removed column: 'last_name'")
        print("Removed column: '1'")
        print('\n')

        print("Checking for NULL values:")
        print("-------------------------")
        
        for col in orders_df.columns:
            print(f"Column '{col}': {orders_df[col].isnull().any()}")

        print('\n')

        return orders_df

    
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