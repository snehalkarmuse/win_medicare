# folder_list = os.listdir("data/fact_data")
# sheet_list = []
# df_list = []

# for folder_name in folder_list:
#     if folder_name != ".DS_Store":
#         list_of_files = os.listdir("data/fact_data/" + folder_name)
#         for file in list_of_files:
#             if file != ".DS_Store":
#                 df = pd.ExcelFile("data/fact_data/"+ folder_name + "/" + file, engine ="openpyxl")
#                 sheet_names = df.sheet_names
#                 for sheet in sheet_names:
#                     sheet_dict = {"sheet_name":sheet}
#                     # print(sheet)
#                     new_df = df.parse(sheet)
#                     new_df.rename(columns = {"PRODUCT": "product_name", "total_amount" : "total_price"}, inplace = True)
                    
#                     new_df.rename(columns = {"product": "product_name","region_id" : "region_name","sec":"quantity","Sec": "quantity", "total_amount" : "total_price","Month":"month","Year":"year", "Inv" : "inv"},inplace = True)
#                     col_list = pd.Series(new_df.columns)
#                     col_list = col_list.str.lower()
                    
#                     col_list = col_list.fillna("product_name")
#                     new_df.columns = col_list
#                     for col in col_list:
#                         if col == 'inv':
#                             new_df.drop(columns = {"inv"},inplace = True)
#                         elif col == "total_amount":
#                             new_df.drop(columns = {"total_amount"},inplace = True)
                        
#                     new_df.drop(columns = {"unit_price"},inplace = True)
                    
#                     new_df.fillna({"quantity" : 0}, inplace = True) 
                    
#                     new_df["quantity"] = new_df["quantity"].astype(int)
#                     # new_df["product_name"] = new_df["product_name"].astype(str)
                    
#                     new_df["year"] = new_df["year"].astype(str)
#                     new_df["year"] = new_df["year"].str[0:4]
#                     new_df["region_name"] = new_df["region_name"].astype(str)
#                     # new_df.replace(to_replace = "MYSPAZ F", value = "MyospasF", inplace = True)
                    
                    
#                     new_df.drop(new_df.loc[new_df["product_name"].str.contains('TOTAL') == True].index, inplace = True, axis = 0)
                    
#                     new_df = new_df.replace(regex=['shrei Laxmi venketesh'],value='Shri Laxmi Venkatesh')
                   
#                     new_df.dropna(subset = ["sales_rep_name","region_name"], inplace = True)
#                     new_df["sales_rep_name"] = new_df["sales_rep_name"].str.strip()
#                     new_df["sales_rep_name"] = new_df["sales_rep_name"].replace("Surveshwer","Surveshawar")
#                     if 'region_name' in new_df.columns:
#                         region_names = []
#                         for r in new_df["region_name"]:
#                             r = r.replace('Parbhanii','Parbhani')
#                             r = r.upper()
#                             region_names.append(r)

#                     new_df["region_id"] = get_data_instance.get_region_id(region_names)
#                     new_df["sales_rep_id"] = get_data_instance.get_sales_rep_code(new_df["sales_rep_name"])
                
                
#                     new_df["product_id"] = new_df["product_name"].replace(" ", "").replace("\t", "").replace("'","")
#                     new_df["product_id"] = new_df["product_id"].astype(str)
#                     print("product name count before dropna ",new_df["product_name"].count())
#                     new_df.dropna(subset = ["product_name","product_id"], inplace = True)
#                     # new_df.replace("MYOSPASF","MyospasF",inplace = True)
                    
#                     # new_df.replace({"MYSPAZF" : "MyospasF", "MyspazF5x10s" : "MyospasF5x10s"},inplace = True)
#                     # new_df.replace({'BetadineSol.10%100ml' : 'BetadineSolution100ml10%',
#                     #                  'BetadineSolution10%100ml': 'BetadineSolution100ml10%' },inplace = True)
                   
#                     prod_id_list = []
#                     for prod in new_df["product_id"]:
#                         prod1 = re.sub('[^A-Za-z0-9]+', '', prod)
#                         # to-do : replace not working.
#                         if prod1 == "MYSPAZF": 
#                             prod1 = "MyospasF"
#                         elif  prod1 == "MYOSPASF":
#                             prod1 = "MyospasF"
#                         prod_id_list.append(prod1)
                    
#                     # print("prod_list count", len(prod_id_list))
#                     # print(sheet, "df shape", new_df.shape[0])

#                     new_df["product_id"] = get_data_instance.get_product_code(prod_id_list)
               
                
                
                

#                     prod_price_dict = {"BetadineSol500ml5":135 ,"BetadineSol1000ml5": 237,"BetadineSScrub50ml75": 91,
#                                     "BetadineSScrub500ml75":344 ,"BetadineOint125gm5":130,"BetadineOint250gm5":244,"BetadineVagTab10s":88,"BetadinePowder10gm":57,
#                                     "BetadineCream20gm5":50,"BetadineSol500ml10":63,"BetadineGargle2Mint50ml":61,"BetadineGargle2Mint100ml":104,
#                                     "BetadineGargleGranules5x5s":49,"BetadineOintment525gm":100,"BetadineOintment1015gm":70,
#                                     "BetadineSol10100ml":49,"BetadineSolution5200ml":49,"BetadineVagDouche":619,"DiclomolTab1010s":638,
#                                     "DiclomolSP1010x10":68,"MyospazForteTabs510s":240,"MyospazTabs1010s":480,"MyospasTH430s":830,
#                                     "MyospasTH830s":430,"MyospasDTabs510s":360,"MyospasET4Tab5x10s":269,"WinaceTabs10x10s":633,
#                                     "WinaceTH4Tab5x10s":789,"WinaceTH8Tab3x10s":865,"UrgendolPTabs10x10":633,"HMTablets4x3x10s":1136,
#                                     "HMGranules5gm10S":1367,"HMInfusion10mlx5s":955,"UdihepFortTabs5X10S":1287,"UdihepTabs33X10S":1358,
#                                     "Gutwin400Tab1010":1270,"Gutwin550Tab1010":1184,"Gutwin200Tab1010":755,"CorionC5000IU1sLGLSI":984, 
#                                     "Gonablok50mg310s":157,"Gonablok100mg310s":223,"Gonablok200mg310s":484,"Contractubex20gm":769,
#                                     "Carnitor500Tabs110s":155,"NusowinPowder200gm":124,"PansoralGel15gm":98,"Sensigel50gm":59,
#                                     "PansoralTeething15gm":80,"PhysiomerSpray135ml":80,"PhysiomerHyperSeaWater135ml":125,
#                                     "PhysiomerIsotBabyMist115ml":127,"MovicolSachet10s":30,"MoviprepSachetKit2s":345,"MovicolPaediatric3685gm":48,
#                                     "MovicolLiquid200ml": 170,"ElgydiumAntip.T.50gm" : 112, "ElgydiumAntip.T.150gm" : 225,
#                                     'BetadineAD60ml':63,"BetadineFirstAidSolution450ml":399, "DiclomolSPTab1010S":68,"MyospasF5x10s":240,
#                                     "HMOASyrup100ml":168,"Carnitor500Tab1x10s": 769,"ElgydiumAntipT50gm":112,"ElgydiumAntipT150gm":225,
#                                     "MyspazF":235,"MyospasF":255,"Movicol101381gm":78,'UdihepTabs1010S':1127,"BetadineFirstAidSol450ml":399,
#                                     "BetadineSolution100ml10":49, "MyospazD510s":360}


               
#                     new_df["unit_price"] = new_df["product_id"].apply(lambda x: prod_price_dict.get(x))
#                     new_df["total_price"] = new_df["unit_price"] * new_df["quantity"]

                
#                     new_df.drop(columns = {"product_name","region_name","sales_rep_name"},inplace = True)
#                     # print(new_df.dtypes)
#                     df_list.append(new_df)
                    

# for s in df_list:
    
#     db_connector.upload_to_fact_data(s,'fact_data')

# filepath = Path('product.csv')  
# filepath.parent.mkdir(parents=True, exist_ok=True)  

# folder_list = os.listdir("data/fact_data")
# sheet_list = []
# for folder_name in folder_list:
#     if folder_name != ".DS_Store":
#         list_of_files = os.listdir("data/fact_data/"+ folder_name)
#         # list_of_files = os.listdir("data/fact_data/Beed")
#         for file in list_of_files:
#             if file != ".DS_Store":
#                 df = pd.ExcelFile("data/fact_data/"+ folder_name + "/" + file, engine ="openpyxl")
#                 # df = pd.ExcelFile("data/fact_data/Beed" + "/" + file, engine ="openpyxl")
#                 sheet_names = df.sheet_names
#                 for sheet in sheet_names:
#                     # sheet_dir = {'sheet_name' : sheet}
#                     # print(sheet_dir)
#                     new_df = df.parse(sheet)
#                     sheet_list.append(new_df)
                    
# prod_id_list = []
# prod_name_list = []

# for sheet in sheet_list:
   
#     sheet.columns = sheet.columns.str.replace(" ","")
#     sheet.columns = sheet.columns.str.lower()
#     sheet.rename(columns={"product": "product_name"},inplace = True)
#     col_list = pd.Series(sheet.columns)
#     col_list = col_list.fillna("product_name")
#     sheet.columns = col_list
#     new_product_list = []
#     sheet.drop(sheet.loc[sheet["product_name"].str.contains("TOTAL") == True].index,inplace = True, axis = 0)
#     # sheet.dropna(inplace = True)
#     sheet.replace(to_replace = "MYOSPAS F", value = "MyospasF", inplace = True)
#     sheet.replace(to_replace = "BetadineSol.500ml", value = 'BetadineSol.500ml5%',inplace = True) 
#     for prod_name in sheet["product_name"]:
#         prod_name_list.append(prod_name)
    
    
    
# product_df = pd.DataFrame(columns = [ "product_name" , "product_id"])   
# product_df["product_name"] = prod_name_list 
# print(product_df["product_name"].count())

# product_df["product_id"] = product_df["product_name"].replace(" ", "").replace("\t", "").replace("'","")
# product_df["product_id"] = product_df["product_id"].astype(str)
# for prod in product_df["product_id"]:
#     prod1 = re.sub('[^A-Za-z0-9]+', '', prod)
#     prod_id_list.append(prod1)    
# product_df["product_id"] = prod_id_list
# product_df.dropna(inplace = True)
# # product_df.to_csv(filepath, index = False) 
# print(product_df["product_id"].count())
# product_df.drop_duplicates(subset = ["product_name","product_id"],inplace = True)
# print(product_df.count())
# # product_df = product_df.drop_duplicates(["product_name"])
