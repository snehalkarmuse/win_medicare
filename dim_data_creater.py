import pandas as pd
import db_connect as dbc
import db_connect as dbc
import data_cleaning as d_clean
import get_data_from_db as gd
import numpy as np

 

def create_product_df(clean_df_list):
        concat_df = pd.concat(clean_df_list)
        concat_df.drop(columns = ["inv","unit_price","total_price","quantity","sales_rep_name","region_name","month","year"],inplace = True, axis = 1)
     
        concat_df.dropna(inplace = True)
        concat_df.drop_duplicates(subset = ["product_id"],inplace = True)
        
        product_df = pd.DataFrame(columns = [ "product_name" , "product_id"])  
        product_df["product_name"] = concat_df["product_name"]
        product_df["product_id"] = concat_df["product_id"]
        
        return product_df

def add_category_in_product_df(product_df):
    
    conditions = [
                (product_df['product_id'].str.startswith('Betadine')),
                (product_df['product_id'].str.startswith("Diclomol")),
                (product_df['product_id'].str.startswith("Myos")),
                (product_df['product_id'].str.startswith("Winace")),
                (product_df['product_id'].str.startswith("Urgendol")),
                (product_df['product_id'].str.startswith('Carnitor')),
                (product_df['product_id'].str.startswith("Contractubex")),
                (product_df['product_id'].str.startswith("HM")),
                (product_df['product_id'].str.startswith("Udihep")),
                (product_df['product_id'].str.startswith("Gutwin")),
                (product_df['product_id'].str.startswith('Corion')),
                (product_df['product_id'].str.startswith("Gonablok")),
                (product_df['product_id'].str.startswith("Movi")),
                (product_df['product_id'].str.startswith("Pansoral")),
                (product_df['product_id'].str.startswith("Sensigel")),
                (product_df['product_id'].str.startswith('Elgydium')),
                (product_df['product_id'].str.startswith("Physiomer")),
                (product_df['product_id'].str.startswith("Nusowin")),
                (product_df['product_id'].str.startswith("Corion"))
                
            ]
    choices = ['ANTISEPTIC', 'ANALGESIC','ANALGESIC','ANALGESIC','ANALGESIC','CARNITOR','DERMATOLOGICALS','GASTRO-INTESTINAL',
                    'GASTRO-INTESTINAL','GASTRO-INTESTINAL','GYNAECOLOGICALS','GYNAECOLOGICALS','MOVICOL','MOVICOL','MOVICOL','MOVICOL',
                    'MOVICOL','NUSOWIN','GYNAECOLOGICALS']
    product_df['product_category'] = np.select(conditions, choices, default='black')
    return product_df

db_connector = dbc.DatabaseConnector()
de = db_connector.init_db_engine()   
get_data_instance = gd.GetData()
get_data_instance.fetch_data(de)
dc = d_clean.DataCleaning()
df_list = dc.get_folder_data()
for df in df_list :
    dc.clean_df(df)
    dc.clean_product_data(df)

product_df = create_product_df(df_list)
new_product_df = add_category_in_product_df(product_df)

region_data = pd.read_excel("data/region.xlsx")
db_connector.upload_to_db(region_data,'dim_region')
sales_rep_data = pd.read_excel("data/sales_rep_info.xlsx")
db_connector.upload_to_db(sales_rep_data,'dim_sales_representative')

db_connector.upload_to_db(new_product_df,'dim_products')

















