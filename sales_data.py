import pandas as pd
import psycopg2
from yaml import safe_load
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
import db_connect as dbc
import get_data_from_db as gd
import re
        
db_connector = dbc.DatabaseConnector()
de = db_connector.init_db_engine()   
get_data_instance = gd.GetData()
get_data_instance.fetch_data(de)

sales_df = pd.ExcelFile("data/data_2017_2019.xlsx")
sheet_names = sales_df.sheet_names
df_list = []

for sheet_name in sheet_names:
    df = sales_df.parse(sheet_name)
    df_list.append(df)

for df in df_list:
    df.drop(df.loc[df["Product"].str.contains(r'[A_Z]') == True].index, inplace =True, axis = 0)
    region_code_list = []
    product_code_list = []
    region_name = df['Zone']
    product_name = df["Product"]
    
    df["region_code"] = get_data_instance.get_region_id(region_name)
    
    df["product_code"] = get_data_instance.get_product_id(product_name)
    print(df["region_code"])
   





