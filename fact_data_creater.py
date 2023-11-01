import pandas as pd
import psycopg2
from yaml import safe_load
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
import db_connect as dbc
import get_data_from_db as gd
import data_cleaning as d_clean


def add_price(fact_data):
    prod_price_dict = {"BetadineSol500ml5":135 ,"BetadineSol1000ml5": 237,"BetadineSScrub50ml75": 91,
                                "BetadineSScrub500ml75":344 ,"BetadineOint125gm5":130,"BetadineOint250gm5":244,"BetadineVagTab10s":88,"BetadinePowder10gm":57,
                                "BetadineCream20gm5":50,"BetadineSol500ml10":63,"BetadineGargle2Mint50ml":61,"BetadineGargle2Mint100ml":104,
                                "BetadineGargleGranules5x5s":49,"BetadineOintment525gm":100,"BetadineOintment1015gm":70,
                                "BetadineSol10100ml":49,"BetadineSolution5200ml":49,"BetadineVagDouche":619,"DiclomolTab1010s":638,
                                "DiclomolSP1010x10":68,"MyospazForteTabs510s":240,"MyospazTabs1010s":480,"MyospasTH430s":830,
                                "MyospasTH830s":430,"MyospasDTabs510s":360,"MyospasET4Tab5x10s":269,"WinaceTabs10x10s":633,
                                "WinaceTH4Tab5x10s":789,"WinaceTH8Tab3x10s":865,"UrgendolPTabs10x10":633,"HMTablets4x3x10s":1136,
                                "HMGranules5gm10S":1367,"HMInfusion10mlx5s":955,"UdihepFortTabs5X10S":1287,"UdihepTabs33X10S":1358,
                                "Gutwin400Tab1010":1270,"Gutwin550Tab1010":1184,"Gutwin200Tab1010":755,"CorionC5000IU1sLGLSI":984, 
                                "Gonablok50mg310s":157,"Gonablok100mg310s":223,"Gonablok200mg310s":484,"Contractubex20gm":769,
                                "Carnitor500Tabs110s":155,"NusowinPowder200gm":124,"PansoralGel15gm":98,"Sensigel50gm":59,
                                "PansoralTeething15gm":80,"PhysiomerSpray135ml":80,"PhysiomerHyperSeaWater135ml":125,
                                "PhysiomerIsotBabyMist115ml":127,"MovicolSachet10s":30,"MoviprepSachetKit2s":345,"MovicolPaediatric3685gm":48,
                                "MovicolLiquid200ml": 170,"ElgydiumAntip.T.50gm" : 112, "ElgydiumAntip.T.150gm" : 225,
                                'BetadineAD60ml':63,"BetadineFirstAidSolution450ml":399, "DiclomolSPTab1010S":68,"MyospasF5x10s":240,
                                "HMOASyrup100ml":168,"Carnitor500Tab1x10s": 769,"ElgydiumAntipT50gm":112,"ElgydiumAntipT150gm":225,
                                "MyspazF":235,"MyospasF":255,"Movicol101381gm":78,'UdihepTabs1010S':1127,"BetadineFirstAidSol450ml":399,
                                "BetadineSolution100ml10":49, "MyospazD510s":360}


    fact_data["unit_price"] = fact_data["product_id"].apply(lambda x: prod_price_dict.get(x))
    fact_data["total_price"] = fact_data["unit_price"] * fact_data["quantity"]
    return fact_data

db_connector = dbc.DatabaseConnector()
de = db_connector.init_db_engine()   
get_data_instance = gd.GetData()
get_data_instance.fetch_data(de)

dc = d_clean.DataCleaning()

df_list = dc.get_folder_data()
for df in df_list:
    clean_df = dc.clean_df(df)
    clean_df = dc.clean_product_data(clean_df)
    fact_data_df = dc.clean_fact_data(clean_df)
    updated_fact_data = add_price(fact_data_df)
    db_connector.upload_to_fact_data(updated_fact_data,'fact_data')



