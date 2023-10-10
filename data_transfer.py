import pandas as pd
import db_connect as dbc
import db_connect as dbc
import get_data_from_db as gd
import os
import numpy as np
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
                for sheet in sheet_names:
                    new_df = df.parse(sheet)
                    sheet_list.append(new_df)
product_df = pd.DataFrame({"product_name" : [], "product_id" : []})

for sheet in sheet_list:
    sheet.columns = sheet.columns.str.replace(" ","")
    sheet.columns = sheet.columns.str.lower()
    sheet.rename(columns={"product": "product_name"},inplace = True)
    col_list = pd.Series(sheet.columns)
    col_list = col_list.fillna("product_name")
    sheet.columns = col_list
    sheet.drop(sheet.loc[sheet["product_name"].str.contains(r'[A_Z]') == True].index,inplace = True, axis = 0)
    spdf = pd.DataFrame({
        "product_name" : sheet["product_name"], "product_id" : sheet["product_name"].str.replace(" ", "").replace("\t", "")})
    product_df = pd.concat([product_df,spdf])
    product_df.dropna(inplace = True)
    
    product_df.replace("BetadineSol.500ml",'BetadineSol.500ml5%',inplace = True)
    product_df = product_df.drop_duplicates(["product_id"])
    conditions = [
        (product_df['product_id'].str.startswith('Betadine')),
        (product_df['product_id'].str.startswith("Diclomol")),
        (product_df['product_id'].str.startswith("Myos")),
        (product_df['product_id'].str.startswith("Winace")),
        (product_df['product_id'].str.startswith("Urgendol")),
        (product_df['product_id'].str.startswith('Carnitor')),
        (product_df['product_id'].str.startswith("Contractubex")),
        (product_df['product_id'].str.startswith("H.M.")),
        (product_df['product_id'].str.startswith("Udihep")),
        (product_df['product_id'].str.startswith("Gutwin")),
        (product_df['product_id'].str.startswith('Corion')),
        (product_df['product_id'].str.startswith("Gonablok")),
        (product_df['product_id'].str.startswith("Movi")),
        (product_df['product_id'].str.startswith("Pansoral")),
        (product_df['product_id'].str.startswith("Sensigel")),
        (product_df['product_id'].str.startswith('Elgydium')),
        (product_df['product_id'].str.startswith("Physiomer")),
        (product_df['product_id'].str.startswith("Nusowin"))
        
        ]
    choices = ['ANTISEPTIC', 'ANALGESIC','ANALGESIC','ANALGESIC','ANALGESIC','CARNITOR','DERMATOLOGICALS','GASTRO-INTESTINAL',
            'GASTRO-INTESTINAL','GASTRO-INTESTINAL','GYNAECOLOGICALS','GYNAECOLOGICALS','MOVICOL','MOVICOL','MOVICOL','MOVICOL',
            'MOVICOL','NUSOWIN']
    product_df['product_category'] = np.select(conditions, choices, default='black')
    

region_data = pd.read_excel("data/region.xlsx")
# print(region_data.shape)
sales_rep_data = pd.read_excel("data/sales_rep_info.xlsx")
# print(sales_rep_data.shape)
db_connector.upload_to_db(product_df,'dim_products')
db_connector.upload_to_db(region_data,'dim_region')
db_connector.upload_to_db(sales_rep_data,'dim_sales_representative')
