import pandas as pd
import psycopg2
from yaml import safe_load
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
import db_connect as dbc
import re

class GetData:

    _region_data = {}
    _product_data = {}
    _sales_rep_data = {}
    def fetch_data(self,db_engine):
        inspector = inspect(db_engine)
        with db_engine.connect() as connection:
            result = connection.execute(text("SELECT region_name, region_id FROM dim_region"))
            self.region_data = result.fetchall()
            
            self.region_data = dict(self.region_data)
            result = connection.execute(text("SELECT product_id, product_id FROM dim_products"))
            self.product_data = result.fetchall()
            self.product_data = dict(self.product_data)
            #print(self.product_data)
            result = connection.execute(text("SELECT sales_rep_name,sales_rep_id FROM dim_sales_representative"))
            self.sales_rep_data = result.fetchall()
            self.sales_rep_data = dict(self.sales_rep_data)
   
    def get_region_id(self,region_name):
        
        region_code_list = []
        region_code_not_in_list = []
        for r in region_name:
            region_code_list.append(self.region_data.get(r))
       
        return region_code_list
          
    def get_product_code(self,product_codes):
        product_code_list = []
        product_not_in_list =[]
        for p in product_codes:
                db_code = self.product_data.get(p)
                if db_code is None :
                    product_not_in_list.append(p)
                else:
                    product_code_list.append(db_code)

        # print(pd.unique(product_not_in_list))

        return product_code_list
    
    def get_sales_rep_code(self,sales_rep_names):
        sales_rep_list = []
        sales_rep__not_in_list = []
        for s in sales_rep_names:
            s_code = self.sales_rep_data.get(s)
            if s_code is None :
                sales_rep__not_in_list.append(s)
            else : 
                sales_rep_list.append(self.sales_rep_data.get(s))
        # print(pd.unique(sales_rep__not_in_list))
        return sales_rep_list

db_connector = dbc.DatabaseConnector()
de = db_connector.init_db_engine()
get_data_instance = GetData()

