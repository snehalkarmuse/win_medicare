import pandas as pd
import db_connect as dbc
import db_connect as dbc
import get_data_from_db as gd
import os
import re



class DataCleaning:
    def __init__(self):
        self.clean_df_list = []
        self.all_record_list = []
        

    def get_folder_data(self):
        folder_list = os.listdir("data/fact_data")
        df_list = []
        for folder_name in folder_list:
            if folder_name != ".DS_Store":
                list_of_files = os.listdir("data/fact_data/"+ folder_name)
                
                for file in list_of_files:
                    if file != ".DS_Store":
                        df = pd.ExcelFile("data/fact_data/"+ folder_name + "/" + file, engine ="openpyxl")
                        
                        df_names = df.sheet_names
                        for sheet in df_names:
                            new_df = df.parse(sheet)
                            df_list.append(new_df)
               
        return df_list
    
    
    def clean_df(self, df):
        df.columns = df.columns.str.replace(" ","")
        df.columns = df.columns.str.lower()
        
        df.rename(columns={"product": "product_name","PRODUCT": "product_name","total_amount" : "total_price"},inplace = True)
        df.rename(columns = {"region_id" : "region_name","sec":"quantity","Sec": "quantity", "total_amount" : "total_price","Month":"month","Year":"year", "Inv" : "inv"},inplace = True)
        col_list = pd.Series(df.columns)
        col_list = col_list.fillna("product_name")
        df.columns = col_list
        df.drop(df.loc[df["product_name"].str.contains("TOTAL") == True].index,inplace = True, axis = 0)
        
        return df
        
    
                                               
    def clean_product_data(self, df):
      
        df["product_name"].replace(regex = "Betadine Cream 20 gm        5%",value = "Betadine Cream 20 gm 5%",inplace = True)
        df["product_name"].replace("Betadine Cream 20 gm 5%","Betadine Cream 20 gm 5%",inplace = True)
        df["product_name"].replace("Betadine First Aid Sol.4 %    50 ml","Betadine First Aid Solution 4%  50ml",inplace = True)
        df["product_name"].replace("Betadine First Aid Sol.4 %    50 ml","Betadine First Aid Solution 4%  50ml",inplace = True)
        df["product_name"].replace("Betadine Ointment 10%    15 gm","Betadine Ointment 10%  15gm",inplace = True)
        df["product_name"].replace("Betadine Ointment 5%       25 gm","Betadine Ointment 5%  25 gm",inplace = True)
        df["product_name"].replace("Betadine Ointment 5%   25gm","Betadine Ointment 5%  25 gm",inplace = True)
        df["product_name"].replace("Betadine Sol. 500ml              5%","Betadine Sol. 500ml    5% ",inplace = True)
        df["product_name"].replace("Betadine Sol. 500ml    5%          ","Betadine Sol. 500ml    5% ",inplace = True)
        df["product_name"].replace("Betadine Sol. 500ml              5%","Betadine Sol. 500ml    5% ",inplace = True)
        df["product_name"].replace("Betadine Sol. 500ml5","Betadine Sol. 500ml    5% ",inplace = True)
        df["product_name"].replace("Betadine Solution 10%  100ml     ","Betadine Sol. 10%  100ml",inplace = True)
        df["product_name"].replace("Betadine Solution 10%  100ml     ","Betadine Sol. 10%  100ml",inplace = True)
        df["product_name"].replace("Betadine Solution 100ml     10%","Betadine Sol. 10%  100ml",inplace = True)
        df["product_name"].replace("Betadine Solution 100ml ¬† ¬† 10%","Betadine Sol. 10%  100ml",inplace = True)
        df["product_name"].replace("Betadine Solution 100ml  10%","Betadine Sol. 10%  100ml",inplace = True)
        df["product_name"].replace("Betadine Sol. 5%    200ml","Betadine Solution 5%         200 ml",inplace = True)
        df["product_name"].replace("Carnitor 500 Tabs  1*10's","Carnitor 500 Tab. 1x10's",inplace = True)
        df["product_name"].replace("Corion - C 5000 I.U","Corion-C  5000 IU-1's ( LGLSI )",inplace = True)
        df["product_name"].replace("Diclomol SP 10  10x10","Diclomol SP Tab. 10*10'S",inplace = True)
        df["product_name"].replace("Diclomol Tab 10*10's","Diclomol SP Tab. 10*10'S",inplace = True)
        df["product_name"].replace("Gutwin 200 Tab 10*1","Gutwin 200 Tab 10*10",inplace = True)
        df["product_name"].replace("H.M. OA Syrup  100 ml","H.M. OA Syrup 100ml",inplace = True)
        df["product_name"].replace("Movicol Sachet  10 s","Movicol Sachet  10's",inplace = True)
        df["product_name"].replace("MyospasF","Myospas  F 5 x 10's",inplace = True)
        df["product_name"].replace("Myospas F 5 x 10's","Myospas  F 5 x 10's",inplace = True)
        df["product_name"].replace("Myospas F Tab 5*10","Myospas  F 5 x 10's",inplace = True)
        df["product_name"].replace("Myospas F    Tab         5x10s","Myospas  F 5 x 10's",inplace = True)
        df["product_name"].replace("Myospas TH4 3*10'S","Myospas TH 4   30's",inplace = True)
        df["product_name"].replace("Myospas TH8 3*10'S","Myospas TH 8   30's",inplace = True)
        df["product_name"].replace("Winace Tabs  10x10's","Winace  Tabs 10x10's",inplace = True)
        df["product_name"].replace("MYSPAZ  F","MyspazF",inplace = True)
        df["product_name"].replace("MYOSPAS F","MyospazF",inplace = True)
        df["product_name"].replace("Myspaz F","MyospazF",inplace = True)
        df["product_name"].replace("Betadine First Aid Sol.4 %¬† ¬† 50 ml","Betadine First Aid Solution 4%  50ml",inplace = True)
        df["product_name"].replace("Betadine Sol. 500ml","BetadineSol500ml5",inplace = True)
        df["product_name"].replace("Myospaz D          5*10's","Myospas-D Tabs. 5*10's",inplace = True)
        df["product_name"].replace("Myospaz D¬† ¬† ¬† ¬† ¬† 5*10's","Myospas-D Tabs. 5*10's",inplace = True)
        df.drop(df.loc[df["product_name"].str.contains("product_name")== True].index,axis = 0,inplace = True)
        # df.drop(df.loc[df["product_name"].str.contains("product_name")== True].index,axis = 0,inplace = True)
        df["product_id"] = df["product_name"].replace(" ", "").replace("\t", "").replace("'","")
        df["product_id"] = df["product_id"].astype(str)
        prod_id_list=[]
        for prod in df["product_id"]:
            prod1 = re.sub('[^A-Za-z0-9]+', '', prod)
            prod_id_list.append(prod1)  
        
        df["product_id"] = prod_id_list
        
        
        df.dropna(subset = ["product_id"], inplace = True)
        df.drop(df.loc[df["product_id"].str.contains("nan")== True].index, inplace = True, axis = 0)
       
        return df

    

    def clean_fact_data(self, fact_data_df):
       
        fact_data_df["year"] = fact_data_df["year"].astype(str)
        fact_data_df["year"] = fact_data_df["year"].str[0:4]
        fact_data_df.fillna({"quantity" : 0}, inplace = True) 
        fact_data_df["quantity"] = fact_data_df["quantity"].astype(int)
        fact_data_df["region_name"] = fact_data_df["region_name"].astype(str)
        if 'region_name' in fact_data_df.columns:
                        region_names = []
                        for r in fact_data_df["region_name"]:
                            r = r.replace('Parbhanii','Parbhani')
                            r = r.upper()
                            region_names.append(r)
        fact_data_df["region_id"] = get_data_instance.get_region_id(region_names)
        fact_data_df.dropna(subset = ["sales_rep_name","region_name"], inplace = True)
        
        fact_data_df["sales_rep_name"] = fact_data_df["sales_rep_name"].str.strip()
        fact_data_df["sales_rep_name"].replace("Surveshwer","Surveshawar",inplace = True)
        fact_data_df.replace(regex=['shrei Laxmi venketesh'],value='Shri Laxmi Venkatesh',inplace = True)
        fact_data_df["sales_rep_id"] = get_data_instance.get_sales_rep_code(fact_data_df["sales_rep_name"])
        fact_data_df.drop(columns = ["inv","unit_price","total_price","product_name","sales_rep_name","region_name"], inplace = True, axis = 1)
        
            
    
        return fact_data_df
    
db_connector = dbc.DatabaseConnector()
de = db_connector.init_db_engine()
get_data_instance = gd.GetData()
get_data_instance.fetch_data(de)

