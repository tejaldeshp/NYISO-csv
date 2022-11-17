import os
import pandas as pd
from zipfile import ZipFile

#unzipping files on path
def process_zipfile(zip_file):
        result = []
        if zip_file.endswith(".zip"):
            zip_files = ZipFile(zip_file)
            zip_list = zip_files.infolist()
            for file in zip_list:
                result.append(zip_files.open(file))
        elif zip_file.endswith(".csv"):
            result.append(zip_file)
        return result

#gives a list of paths of csv files
def get_csvfile_path(csvpath):
    filelist =[]
    dirs = os.listdir(csvpath)
    for d in dirs:
        if d == ".DS_Store":
            continue
        zip_files = os.listdir(os.path.join(csvpath, d))
        for zip_file in zip_files:
            filelist=filelist+ process_zipfile(os.path.join(os.path.join(csvpath, d), zip_file))
    return filelist

def clean_data(path):
    df = pd.read_csv(path, usecols=["Time Stamp", "Name", "LBMP ($/MWHr)"])
    df.rename(columns={"Time Stamp": "fordate","Name": "node","LBMP ($/MWHr)": "dam",},inplace=True,)
    df["fordate"] = pd.to_datetime(df["fordate"]).dt.tz_localize(
        "US/Eastern", ambiguous=True
    )
    df["fordate"] = df["fordate"].dt.tz_convert("UTC")
    df=df.set_index("fordate")

    if 'realtime' in str(path):
        df.rename(columns={"dam": "rtm"},inplace=True,)
        df = df.groupby("node").resample("H").mean(numeric_only=True).reset_index()
        df=df.set_index("fordate")
    return df

# further clean data and save as csv
def save_as_csv(df, filename):
    df["dart"] = df["dam"] - df["rtm"]
    df = df.round(decimals=4)
    df.to_csv(os.path.join("/home/tejaldeshpande/Desktop/DjangoProjects/NYISOfolder/sample", filename + ".csv"))

root = "/home/tejaldeshpande/Desktop/DjangoProjects/nyisozipsample"
paths = os.listdir(root)
csv_paths=[]
for path in paths:
    csv_paths.append(get_csvfile_path(os.path.join(root,path)))
genlist =[]
zonlist =[]
for i in range(4):
    dflist = []
    for path in csv_paths[i]:
        dflist.append(clean_data(path))
    print(len(dflist))
    if "gen" in csv_paths[i][0].name:
        genlist.append(pd.concat(dflist))
    else:
        zonlist.append(pd.concat(dflist))
pd_zonal = pd.merge(zonlist[0], zonlist[1], how ="outer", left_on=["fordate", "node"], right_on=["fordate","node"]) # without outer causing dates with only part data dam/rtm to be skipped
pd_gen = pd.merge(genlist[0], genlist[1], how = "outer", left_on=["fordate", "node"], right_on=["fordate","node"]) # without outer causing dates with only part data dam/rtm to be skipped
df = pd.concat([pd_zonal, pd_gen])
df = df.sort_values(by=['node','fordate'])
save_as_csv(df, "NYISO")


