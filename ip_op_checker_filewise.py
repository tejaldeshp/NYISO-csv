import os
import pandas as pd

ip_location = "/home/tejaldeshpande/Desktop/DjangoProjects/nyisosample/dam/20001201damlbmp_gen.csv"
op_location = "/home/tejaldeshpande/Desktop/DjangoProjects/NYISOfolder/nodewise_sample"

#get a nodelist of the input file
nodelist=[]
ip_df = pd.read_csv(ip_location, usecols=["Time Stamp", "Name", "LBMP ($/MWHr)"])
ip_df.rename(columns={"Time Stamp": "fordate","Name": "node","LBMP ($/MWHr)": "dam",},inplace=True,)
ip_df["fordate"] = pd.to_datetime(ip_df["fordate"]).dt.tz_localize(
        "US/Eastern", ambiguous=True
    )
ip_df["fordate"] = ip_df["fordate"].dt.tz_convert("UTC")
nodes = ip_df.groupby("node").nunique()
nodelist = nodelist + nodes.index.to_list()
nodelist=list(set(nodelist))

check=True
check_col="dam"

if "realtime" in ip_location:
    ip_df=ip_df.set_index("fordate")
    ip_df.rename(columns={"dam": "rtm",},inplace=True,)
    ip_df = ip_df.groupby("node").resample("H").mean(numeric_only=True).reset_index()
    ip_df = ip_df.round(decimals = 4)
    check_col="rtm"


for node in nodelist:
    op_df = pd.read_csv(os.path.join(op_location, node + ".csv"), names=["fordate", "node", "dam", "rtm", "dart"], header=None)
    op_df['fordate'] = op_df['fordate'].astype('datetime64[ns]')
    #get those records with current node
    filedate = ip_df["fordate"].dt.date.values[0]
    df_ip = ip_df[ip_df['node']==node]
    df_ip = df_ip[df_ip["fordate"].dt.date == filedate]
    df_op = op_df[op_df['fordate'].dt.date == filedate]
    if df_ip[check_col].sum() == df_op[check_col].sum():
        check=True
    else:
        check=False
        print(df_ip)
        print(df_op)
        break
print(check)
