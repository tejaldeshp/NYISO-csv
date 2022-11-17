import os
import pandas as pd
from zipfile import ZipFile

#unzipping files on path
def process_zipfile(zip_file):
        result = []  
        print(zip_file)  
        if zip_file.endswith(".zip"):
            [result.append(ZipFile(zip_file).open(x)) for x in ZipFile(zip_file).infolist()]
        elif zip_file.endswith(".csv"):
            result.append(zip_file)
        print(result)
        return result

#gives a list of paths of csv files
def get_csvfile_path(csvpath):
    filelist = []
    for root, dirs, files in os.walk(csvpath):
        filelist.extend([process_zipfile(csvpath+ '/'+ x) for x in files if x.endswith(".zip") or x.endswith(".csv")])
    return filelist

#clean data and return dataframe
def clean_data(path):
    df = pd.read_csv(path)
    df = pd.DataFrame.assign(fordate=df["Time Stamp"], node=df["Name"], dam=df["LBMP ($/MWHr)"])
    df["fordate"] = pd.to_datetime(df["fordate"]).dt.tz_localize(
        "US/Eastern", ambiguous=True
    )
    df["fordate"] = df["fordate"].dt.tz_convert("UTC")
    df=df.set_index("fordate")
    if 'realtime' in str(path):
        df.rename(columns={"dam": "rtm",},inplace=True,)
        df = df.groupby("node").resample("H").mean(numeric_only=True).reset_index()
        df=df.set_index("fordate")
    return df

# further clean data and save as csv
def save_as_csv(df, filename):
    df["dart"] = df["dam"] - df["rtm"]
    df = df.round(decimals=4)
    df.to_csv(os.path.join("sample", filename + ".csv"))


root = "C:/Users/tejal_q4bz1lv/Documents/DjangoProjects/NYISOzipsample"
paths = os.listdir(root)
csv_paths=[]
for path in paths:
    csv_paths = csv_paths + get_csvfile_path(root+'/'+path)
print(csv_paths)









