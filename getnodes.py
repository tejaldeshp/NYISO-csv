import pandas as pd
import os
import numpy as nm

# from django.conf import settings



node_list_zonal = []
node_list_generator = []
da_zonal_path = "C:/Users/tejal_q4bz1lv/Documents/DjangoProjects/NYISOf/Day-Ahead Market LBMP - Zonal/2022-10-01/20221001damlbmp_zone.csv"
da_generator_path = "C:/Users/tejal_q4bz1lv/Documents/DjangoProjects/NYISOf/Day-Ahead Market LBMP - Generator/2022-10-01/20221001damlbmp_gen.csv"
rt_zonal_path = "C:/Users/tejal_q4bz1lv/Documents/DjangoProjects/NYISOf/Real-Time Market LBMP - Zonal/2022-10-01/20221001realtime_zone.csv"
rt_generator_path = "C:/Users/tejal_q4bz1lv/Documents/DjangoProjects/NYISOf/Real-Time Market LBMP - Generator/2022-10-01/20221001realtime_gen.csv"
zonal_paths=[da_zonal_path,rt_zonal_path]
generator_paths = [da_generator_path,rt_generator_path]

node_list=[]
for path in zonal_paths:
    df = pd.read_csv(path)
    df = df.loc[:, ["Time Stamp", "Name", "LBMP ($/MWHr)"]]
    df.rename(columns={"Time Stamp": "fordate","Name": "node","LBMP ($/MWHr)": "dam",},inplace=True,)
    nodes = df.groupby("node").nunique()
    node_list = node_list + nodes.index.to_list()

nodelist=list(set(node_list))
nodelist.sort()
node_list_zonal=nodelist
print(len(node_list_zonal))

node_list=[]
for path in generator_paths:
    df = pd.read_csv(path)
    df = df.loc[:, ["Time Stamp", "Name", "LBMP ($/MWHr)"]]
    df.rename(columns={"Time Stamp": "fordate","Name": "node","LBMP ($/MWHr)": "dam",},inplace=True,)
    nodes = df.groupby("node").nunique()
    node_list = node_list + nodes.index.to_list()

nodelist=list(set(node_list))
nodelist.sort()
node_list_generator=nodelist
print(len(node_list_generator))



for node in node_list_zonal:
    df1=pd.read_csv(da_zonal_path)
    df1 = df1.loc[:, ["Time Stamp", "Name", "LBMP ($/MWHr)"]]
    df1.rename(columns={"Time Stamp": "fordate","Name": "node","LBMP ($/MWHr)": "dam",},inplace=True,)
    df1["fordate"] = pd.to_datetime(df1["fordate"])
    df1=df1.set_index("fordate")
    df1 = df1[df1["node"] == node]


    df = pd.read_csv(rt_zonal_path)
    df = df.loc[:, ["Time Stamp", "Name", "LBMP ($/MWHr)"]]
    df.rename(columns={"Time Stamp": "fordate","Name": "node","LBMP ($/MWHr)": "rtm",},inplace=True,)
    df["fordate"] = pd.to_datetime(df["fordate"])
    df=df.set_index("fordate")
    df = df.groupby("node").resample("H").mean(numeric_only=True).reset_index()
    df = df[df["node"] == node]
    # print(df)
    
    if df.empty == False or df1.empty == False:
        df1= df1.merge(df, how="outer")
        df1 = df1.set_index("fordate")
        decimals = 4    
        # df1['rtm'] = df1['rtm'].apply(lambda x: round(x, decimals))
        df1["rtm"] = df1["rtm"].round(decimals=4)
        df1['dart'] = df1['dam']-df1['rtm']
        df1['dart'] = df1['dart'].apply(lambda x: round(x, decimals))
        filename = node.upper() + ".csv"
        path = os.path.join("ny_files", filename)
        # df1.to_csv(path)


for node in node_list_generator:
    df1=pd.read_csv(da_generator_path)
    df1 = df1.loc[:, ["Time Stamp", "Name", "LBMP ($/MWHr)"]]
    df1.rename(columns={"Time Stamp": "fordate","Name": "node","LBMP ($/MWHr)": "dam",},inplace=True,)
    df1["fordate"] = pd.to_datetime(df1["fordate"])
    df1=df1.set_index("fordate")
    df1 = df1[df1["node"] == node]


    df = pd.read_csv(rt_generator_path)
    df = df.loc[:, ["Time Stamp", "Name", "LBMP ($/MWHr)"]]
    df.rename(columns={"Time Stamp": "fordate","Name": "node","LBMP ($/MWHr)": "rtm",},inplace=True,)
    df["fordate"] = pd.to_datetime(df["fordate"])
    df=df.set_index("fordate")
    df = df.groupby("node").resample("H").mean(numeric_only=True).reset_index()
    df = df[df["node"] == node]
    # print(df)
    
    if df.empty == False or df1.empty == False:
        df1= df1.merge(df, how="outer")
        df1 = df1.set_index("fordate")
        decimals = 4    
        # df1['rtm'] = df1['rtm'].apply(lambda x: round(x, decimals))
        df1["rtm"] = df1["rtm"].round(decimals=4)
        df1['dart'] = df1['dam']-df1['rtm']
        df1['dart'] = df1['dart'].apply(lambda x: round(x, decimals))
        filename = node.upper() + ".csv"
        path = os.path.join("ny_files", filename)
        # df1.to_csv(path)





    # df["fordate"] = df["fordate"].dt.tz_localize(
    #                     "US/Eastern", ambiguous=True
    #                 )
    # df["fordate"] = df["fordate"].dt.tz_convert("UTC")

    # df = df[df["node"] == node]
    
    # print(node, df)