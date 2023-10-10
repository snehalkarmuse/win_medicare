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
    
    def fetch_data(self,db_engine):
        inspector = inspect(db_engine)
        with db_engine.connect() as connection:
            result = connection.execute(text("SELECT region_name, region_id FROM dim_region"))
            self.region_data = result.fetchall()
            
            self.region_data = dict(self.region_data)
            result = connection.execute(text("SELECT product_name,product_id FROM dim_products"))
            self.product_data = result.fetchall()
            self.product_data = dict(self.product_data)
            result = connection.execute(text("SELECT sales_rep_name,sales_rep_id FROM dim_sales_representative"))
            self.sales_rep_data = result.fetchall()
            self.sales_rep_data = dict(self.sales_rep_data)
            # print(self.sales_rep_data)

    

    def get_region_id(self,region_name):
        region_code_list = []
        region_code_not_in_list = []
        for r in region_name:
            region_code_list.append(self.region_data.get(r))
        
        return region_code_list
          
    def get_product_code(self,product_names):
        product_code_list = []
        for p in product_names:
            product_code_list.append(self.product_data.get(p))
           
        return product_code_list
    
    def get_sales_rep_code(self,sales_rep_names):
        sales_rep_list = []
        sales_rep__not_in_list = []
        for s in sales_rep_names:
            sales_rep_list.append(self.sales_rep_data.get(s))
        # print(pd.unique(sales_rep_list))
        return sales_rep_list

    # def get_product_price(self,product_id_list):
    #     for prod_id in product_id_list:
            
    '''new_df.loc[new_df["product_id"] == "BetadineSol.1000ml5%","unit_price"] = 237
                new_df.loc[new_df["product_id"] == "BetadineS.Scrub50ml7.5%","unit_price"] = 91
                new_df.loc[new_df["product_id"] == "BetadineS.Scrub500ml7.5%","unit_price"] = 344
                new_df.loc[new_df["product_id"] == "BetadineOint.125gm5%","unit_price"] = 130
                new_df.loc[new_df["product_id"] == "BetadineOint.250gm5%","unit_price"] = 244
                new_df.loc[new_df["product_id"] == "BetadineVag.Tab10's","unit_price"] = 88
                new_df.loc[new_df["product_id"] == "BetadinePowder10gm","unit_price"] = 57
                new_df.loc[new_df["product_id"] == "BetadineCream20gm5%","unit_price"] = 50
                new_df.loc[new_df["product_id"] == "BetadineSol.500ml10%","unit_price"] = 223
                new_df.loc[new_df["product_id"] == "BetadineGargle2%Mint50ml","unit_price"] = 61
                new_df.loc[new_df["product_id"] == "BetadineGargle2%Mint100ml","unit_price"] = 104
                new_df.loc[new_df["product_id"] == "BetadineGargleGranules5x5's","unit_price"] = 49
                new_df.loc[new_df["product_id"] == "BetadineOintment5%25gm","unit_price"] = 100
                new_df.loc[new_df["product_id"] == "BetadineOintment10%15gm","unit_price"] = 70
                new_df.loc[new_df["product_id"] == "BetadineSolution100ml10%","unit_price"] = 49
                new_df.loc[new_df["product_id"] == "BetadineSolution5%200ml","unit_price"] = 49
                new_df.loc[new_df["product_id"] == "BetadineVag.Douche","unit_price"] = 619
                new_df.loc[new_df["product_id"] == "DiclomolTab10*10's","unit_price"] = 638
                new_df.loc[new_df["product_id"] == "DiclomolSP1010x10","unit_price"] = 68
                new_df.loc[new_df["product_id"] == "MyospazForteTabs5*10's","unit_price"] = 240
                new_df.loc[new_df["product_id"] == "MyospazTabs10*10's","unit_price"] = 480
                new_df.loc[new_df["product_id"] == "MyospasTH430's","unit_price"] = 520
                new_df.loc[new_df["product_id"] == "MyospasTH830's","unit_price"] = 830
                new_df.loc[new_df["product_id"] == "MyospazD5*10's","unit_price"] = 240
                new_df.loc[new_df["product_id"] == "MyospasET4Tab5x10's","unit_price"] = 360
                new_df.loc[new_df["product_id"] == "WinaceTabs10x10's","unit_price"] = 633
                new_df.loc[new_df["product_id"] == "WinaceTH4Tab5x10's","unit_price"] = 789
                new_df.loc[new_df["product_id"] == "WinaceTH8Tab3x10's","unit_price"] = 865
                new_df.loc[new_df["product_id"] == "UrgendolPTabs10x10","unit_price"] = 863
                new_df.loc[new_df["product_id"] == "H.M.Tablets4x3x10's","unit_price"] = 1136

                new_df.loc[new_df["product_id"] == "H.M.Granules5gm10'S","unit_price"] = 1367
                new_df.loc[new_df["product_id"] == "H.M.Infusion10mlx5's","unit_price"] = 955
                new_df.loc[new_df["product_id"] == "UdihepFortTabs5X10'S","unit_price"] = 1287
                new_df.loc[new_df["product_id"] == "UdihepTabs3*3X10'S","unit_price"] = 1358
                new_df.loc[new_df["product_id"] == "Gutwin400Tab10*10","unit_price"] = 1270
                new_df.loc[new_df["product_id"] == "Gutwin550Tab10*10","unit_price"] = 1184
                new_df.loc[new_df["product_id"] == "Gutwin200Tab10*10","unit_price"] = 755
                new_df.loc[new_df["product_id"] == "Gonablok50mg3*10's","unit_price"] = 157
                new_df.loc[new_df["product_id"] == "Gonablok100mg3*10's","unit_price"] = 223
                new_df.loc[new_df["product_id"] == "Gonablok200mg3*10's","unit_price"] = 484
                new_df.loc[new_df["product_id"] == "Contractubex20gm","unit_price"] = 769
                new_df.loc[new_df["product_id"] == "Carnitor500Tabs1*10's","unit_price"] = 155
                new_df.loc[new_df["product_id"] == "NusowinPowder200gm","unit_price"] = 70
                new_df.loc[new_df["product_id"] == "PansoralGel15gm","unit_price"] = 30
                new_df.loc[new_df["product_id"] == "Sensigel50gm","unit_price"] = 59
                new_df.loc[new_df["product_id"] == "PansoralTeething15gm","unit_price"] = 50
                new_df.loc[new_df["product_id"] == "PhysiomerSpray135ml","unit_price"] = 80
                new_df.loc[new_df["product_id"] == "PhysiomerHyper.SeaWater135ml","unit_price"] = 125
                new_df.loc[new_df["product_id"] == "PhysiomerIsot.BabyMist115ml","unit_price"] = 127
                new_df.loc[new_df["product_id"] == "MovicolSachet10's","unit_price"] = 30
                new_df.loc[new_df["product_id"] == "MoviprepSachetKit2's","unit_price"] = 40
                new_df.loc[new_df["product_id"] == "MovicolPaediatric3*6.85gm","unit_price"] = 48
                new_df.loc[new_df["product_id"] == "MovicolLiquid200ml","unit_price"] = 170
                new_df.loc[new_df["product_id"] == "MyospasF5x10's","unit_price"] = 240
                new_df.loc[new_df["product_id"] == "ElgydiumAntip.T.50gm","unit_price"] = 112
                new_df.loc[new_df["product_id"] == "ElgydiumAntip.T.150gm","unit_price"] = 225'''
        return product_price

db_connector = dbc.DatabaseConnector()
de = db_connector.init_db_engine()
get_data_instance = GetData()

