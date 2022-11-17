import os
import pandas as pd
from zipfile import ZipFile
from datetime import datetime, timedelta

#unzipping files on path
def process_zipfile(zip_file):
        """
        Returns a list of unzipped files from a zip file
        """
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
    datelist=[]
    dirs = os.listdir(csvpath)
    dirs.sort()
    for d in dirs:
        if d == ".DS_Store":
            continue
        path =os.path.join(csvpath, d)
        zip_files = os.listdir(path)
        for zip_file in zip_files:
            unzipped_files = process_zipfile(os.path.join(path, zip_file))
            filelist=filelist+unzipped_files
    # print(len(filelist))
    # print(datelist)
    # print(sum(datelist))
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

# get cleaned data from folder of that iso
# def get_cleaned_data(root, default="C:/Users/tejal_q4bz1lv/Documents/DjangoProjects/NYISOzipsample"):
root = "C:/Users/tejal_q4bz1lv/Documents/DjangoProjects/NYISOzipsample"
paths = os.listdir(root)
csv_paths=[]
for path in paths:
    csv_paths.append(get_csvfile_path(os.path.join(root,path)))

startdate = datetime.strptime(csv_paths[0][0].filename[0:8], "%Y%m%d")
#find paths of same date
for i in range(len(csv_paths[0])):
    #find files in each elementlist with the same date and call clean data on it
    pattern = startdate.strftime("%Y%m%d")
    pathlist=[]
    for j in range(len(csv_paths)):
        pathlist = pathlist + [x for x in csv_paths[j] if x.filename[0:8]==pattern]
    print(pathlist)

    dflist=[]
    for path in pathlist:
        print(path)
        dflist.append(clean_data(path))
    print(dflist)

    startdate = startdate + timedelta(day=1)
    break








        # if str(csv_paths[i])[26:34] == str(csv_paths[i])[26:34] and str(da_gen_paths[i])[26:34] == str(rt_gen_paths[i])[26:34]:
        #     filename = str(da_zonal_paths[i])[26:34]
        #     da_zonal_df = clean_data(da_zonal_paths[i])
        #     rt_zonal_df = clean_data(rt_zonal_paths[i])
        #     da_gen_df = clean_data(da_gen_paths[i])
        #     rt_gen_df = clean_data(rt_gen_paths[i])

        #     #merge zonal dataframes & generator dataframes respectively
        #     df_zonal = pd.merge(da_zonal_df, rt_zonal_df, left_on=["fordate", "node"], right_on=["fordate","node"])
        #     print(df_zonal)
        #     df_gen = pd.merge(da_gen_df, rt_gen_df, left_on=["fordate", "node"], right_on=["fordate","node"])
        #     print(df_gen)
        #     #concat & save
        #     df = pd.concat([df_zonal, df_gen])
        #     print(df)
        #     df = df.sort_values(by=['node','fordate'])
        #     save_as_csv(df, filename)
            
