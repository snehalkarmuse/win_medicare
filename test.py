import pandas as pd
import psycopg2
from yaml import safe_load
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
import db_connect as dbc
import get_data_from_db as gd
import re
import os
db_connector = dbc.DatabaseConnector()
de = db_connector.init_db_engine()   
get_data_instance = gd.GetData()
get_data_instance.fetch_data(de)

folder_list = os.listdir("data/fact_data")
sheet_list = []


for folder_name in folder_list:
    if folder_name != ".DS_Store":
        list_of_files = os.listdir("data/fact_data/"+ folder_name)
        for file in list_of_files:
            if file != ".DS_Store":
                df = pd.ExcelFile("data/fact_data/"+ folder_name + "/" + file, engine ="openpyxl")
                sheet_names = df.sheet_names
                print(sheet_names.dtypes)
                for sheet in sheet_names:
                    new_df = df.parse(sheet)
                    sheet_list.append(new_df)
                    
for sheet in sheet_list:
    # sheet.columns = sheet.columns.str.replace(" ","")
    # sheet.columns = sheet.columns.str.replace("\t","")
    # sheet.columns = sheet.columns.str.lower()
    # sheet.rename(columns={"product": "product_name","region_id" : "region_name"},inplace = True)
    # col_list = pd.Series(sheet.columns)
    # col_list = col_list.fillna("product_name")
    # sheet.columns = col_list
    sheet.drop(sheet.loc[sheet["product_name"].str.contains(r'[A_Z]') == True].index,inplace = True, axis = 0)
    sheet.fillna({"sec": 0, "Inv" : 0, "Total_price": 0 })
    sheet.dropna(inplace = True)

    if 'region_name' in sheet.columns:
        region_names = []
        product_names = []
        sales_rep_names = []
        
        for r in sheet["region_name"]:
            r = r.replace('Parbhanii','Parbhani')
            r = r.upper()
            region_names.append(r)
            
    sheet["region_id"] = get_data_instance.get_region_id(region_names)
    
    # product_names = sheet["product_name"]
    # sheet["product_id"] = get_data_instance.get_product_code(product_names)

    # for s in sheet["sales_rep_name"]:
    #     s = s.replace("shrei Laxmi venketesh","Shri Laxmi venkatesh")
    #     sales_rep_names.append(s)
    # sheet["sales_rep_id"] = get_data_instance.get_sales_rep_code(sales_rep_names)

    # sheet.drop(columns = {"product_name","region_name","sales_rep_name","inv","unit_price"},inplace = True)
    # db_connector.upload_to_db(sheet,'fact_data')

