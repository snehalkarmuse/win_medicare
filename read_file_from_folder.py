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
                for sheet in sheet_names:
                    sheet_dict = {"sheet_name":sheet}
                    # print(sheet)
                    new_df = df.parse(sheet)
                    # print(new_df.shape[0])
                    new_df.columns = new_df.columns.str.replace(" ","")
                    new_df.columns = new_df.columns.str.replace("\t","")
                    new_df.columns = new_df.columns.str.lower()
                    
                    new_df.rename(columns = {"product": "product_name","region_id" : "region_name","sec":"quantity"},inplace = True)
                    col_list = pd.Series(new_df.columns)
                    col_list = col_list.fillna("product_name")
                    new_df.columns = col_list
                    
                    new_df.drop(new_df.loc[new_df["product_name"].str.contains(r'[A_Z]') == True].index,inplace = True, axis = 0)
                    new_df.fillna({"sec": 0, "inv" : 0 })
                    
                    new_df.dropna(inplace = True)
                    if 'region_name' in new_df.columns:
                        region_names = []
                        product_names = []
                        sales_rep_names = []
                    
                        for r in new_df["region_name"]:
                            r = r.replace('Parbhanii','Parbhani')
                            r = r.upper()
                            region_names.append(r)
                
                new_df["region_id"] = get_data_instance.get_region_id(region_names)
                
                product_names = new_df["product_name"]
                new_df["product_id"] = get_data_instance.get_product_code(product_names)

                for s in new_df["sales_rep_name"]:
                    s = s.replace("shrei Laxmi venketesh","Shri Laxmi venkatesh")
                    sales_rep_names.append(s)
                # print(pd.unique(sales_rep_names))    
                new_df["sales_rep_id"] = get_data_instance.get_sales_rep_code(sales_rep_names)
                new_df.drop(columns = {"unit_price","total_price"},inplace = True)

                prod_price_dict = {"BetadineSol.1000ml5%" : 237, "BetadineS.Scrub50ml7.5%" : 91, "BetadineS.Scrub500ml7.5%" : 344,
        "BetadineOint.125gm5%" : 130, "BetadineOint.250gm5%" : 244, "BetadineVag.Tab10's" : 88, "BetadinePowder10gm" : 57,
        "BetadineCream20gm5%" : 50, "BetadineSol.500ml10%" : 223, "BetadineGargle2%Mint50ml" : 61, "BetadineGargleGranules5x5's": 104,
        "BetadineGargleGranules5x5's" : 49, "BetadineOintment5%25gm" : 100, "BetadineOintment10%15gm" : 70,
        "BetadineSolution100ml10%" : 49, "BetadineSolution5%200ml" : 619, "DiclomolTab10*10's": 638, "DiclomolSP1010x10" :68,
        "MyospazForteTabs5*10's" : 240, "MyospazTabs10*10's" : 480, "MyospasTH430's": 830, "MyospazD5*10's" : 240,
        "MyospasET4Tab5x10's" : 360, "WinaceTabs10x10's" : 633, "WinaceTH4Tab5x10's" : 789,  "WinaceTH8Tab3x10's" : 865,
        "WinaceTH8Tab3x10's" : 863, "H.M.Tablets4x3x10's" : 1136, "H.M.Granules5gm10'S" : 1367, "H.M.Infusion10mlx5's" : 955,
        "UdihepFortTabs5X10'S" : 1287, "UdihepTabs3*3X10'S" : 1358, "Gutwin400Tab10*10" : 1270, "Gutwin550Tab10*10" : 1184,
        "Gutwin200Tab10*10" : 755, "Gonablok50mg3*10's" : 157, "Gonablok100mg3*10's" : 223, "Gonablok200mg3*10's" : 484,
        "Contractubex20gm" : 769, "Carnitor500Tabs1*10's" : 155, "NusowinPowder200gm" : 70, "PansoralGel15gm" : 30,
        "Sensigel50gm" : 59, "PansoralTeething15gm" : 50, "PhysiomerSpray135ml" : 80, "PhysiomerHyper.SeaWater135ml" : 125,
        "PhysiomerIsot.BabyMist115ml" : 127, "MovicolSachet10's" : 30, "MoviprepSachetKit2's" : 40 , 
        "MovicolPaediatric3*6.85gm" : 48, "MovicolLiquid200ml" : 170, "MyospasF5x10's" : 240, "ElgydiumAntip.T.50gm" : 112,
        "ElgydiumAntip.T.150gm" : 225}  
                new_df["unit_price"] = new_df["product_id"].apply(lambda x: prod_price_dict.get(x))
                new_df["total_price"] = new_df["unit_price"] * new_df["quantity"]

                # new_df[index_value,'unit_price'] = 224
                new_df["total_price"] = new_df["quantity"] * new_df["unit_price"]
                new_df.drop(columns = {"product_name","region_name","sales_rep_name","inv","unit_price"},inplace = True)
                
                       
                sheet_list.append(new_df)
                

for s in sheet_list:
    
    db_connector.upload_to_fact_data(s,'fact_data')

    

