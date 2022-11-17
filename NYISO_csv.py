import os
import pandas as pd

#gives a list of paths of csv files
def get_csvfiles(csvpath):
    pathlist=[]
    paths=[]

    dirs = os.listdir(csvpath)
    dirs.sort()
    for d in dirs:
        paths.append(os.path.join(csvpath, d))
    for path in paths:
        files = os.listdir(path)
        for file in files:
            pathlist.append(os.path.join(path, file))
    return pathlist

#gives a list of nodes of given files
def get_nodes(csvfile_da,csvfile_rt):
    nodelist=[]
    da_df = pd.read_csv(csvfile_da)
    nodes = da_df.groupby("Name").nunique()
    nodelist = nodelist + nodes.index.to_list()

    rt_df = pd.read_csv(csvfile_rt)
    nodes = rt_df.groupby("Name").nunique()
    nodelist = nodelist + nodes.index.to_list()

    nodelist = list(set(nodelist))
    nodelist.sort()
    return nodelist

#open each file take relevant data of that node and return a df of all the data of that node

def get_node_info(paths, node):

    results=[]
    for path in paths:
        df = pd.read_csv(path)
        df = df.loc[:, ["Time Stamp", "Name", "LBMP ($/MWHr)"]]
        
        if 'realtime' in path:
            df.rename(columns={"Time Stamp": "fordate","Name": "node","LBMP ($/MWHr)": "rtm",},inplace=True,)
            df["fordate"] = pd.to_datetime(df["fordate"])
            df["fordate"] = pd.to_datetime(df["fordate"]).dt.tz_localize(
                "US/Eastern", ambiguous=True
            )
            df["fordate"] = df["fordate"].dt.tz_convert("UTC")
            df=df.set_index("fordate")
            df = df.groupby("node").resample("H").mean(numeric_only=True).reset_index()
            df=df.set_index("fordate")
        else:
            df.rename(columns={"Time Stamp": "fordate","Name": "node","LBMP ($/MWHr)": "dam",},inplace=True,)
            df["fordate"] = pd.to_datetime(df["fordate"])
            df["fordate"] = pd.to_datetime(df["fordate"]).dt.tz_localize(
                "US/Eastern", ambiguous=True
            )
            df["fordate"] = df["fordate"].dt.tz_convert("UTC")
            df=df.set_index("fordate")

        df = df[df["node"] == node]
        results.append(df)
    return pd.concat(results) if len(results) > 0 else pd.DataFrame()

def save_as_csv(df, node):
    df["rtm"] = df["rtm"].round(decimals=4)
    df["dart"] = df["dam"] - df["rtm"]
    df["dart"] = df["dart"].round(decimals=4)
    filename = node.upper() + ".csv"
    path = os.path.join("sample", filename)
    df.to_csv(path)


#paths of folders
da_gen = "C:/Users/tejal_q4bz1lv/Documents/DjangoProjects/NYISOf/Day-Ahead Market LBMP - Generator"
rt_gen = "C:/Users/tejal_q4bz1lv/Documents/DjangoProjects/NYISOf/Real-Time Market LBMP - Generator"
da_zonal = "C:/Users/tejal_q4bz1lv/Documents/DjangoProjects/NYISOf/Day-Ahead Market LBMP - Zonal"
rt_zonal = "C:/Users/tejal_q4bz1lv/Documents/DjangoProjects/NYISOf/Real-Time Market LBMP - Zonal"

#get the csv files on these paths
da_gen_paths = get_csvfiles(da_gen)
rt_gen_paths = get_csvfiles(rt_gen)
da_zonal_paths = get_csvfiles(da_zonal)
rt_zonal_paths = get_csvfiles(rt_zonal)

#get the generator and zonal nodes
gen_nodes = get_nodes(da_gen_paths[0], rt_gen_paths[0])
zonal_nodes = get_nodes(da_zonal_paths[0], rt_zonal_paths[0])

#get dataframe for nodes in zonals and save to csv
for node in zonal_nodes:
    da_zonal_df = get_node_info(da_zonal_paths, node)
    rt_zonal_df = get_node_info(rt_zonal_paths, node)
    df = pd.merge(da_zonal_df, rt_zonal_df, left_on=["fordate", "node"], right_on=["fordate","node"])
    save_as_csv(df, node)


#get dataframe for nodes in zonals and save to csv
for node in gen_nodes:
    da_gen_df = get_node_info(da_gen_paths, node)
    rt_gen_df = get_node_info(rt_gen_paths, node)
    df = pd.merge(da_gen_df, rt_gen_df, left_on=["fordate", "node"], right_on=["fordate","node"])
    save_as_csv(df, node)





