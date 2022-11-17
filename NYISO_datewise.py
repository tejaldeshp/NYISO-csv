import os
import pandas as pd
from zipfile import ZipFile

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
            print(unzipped_files)
            filelist=filelist+unzipped_files
            datelist.append(len(unzipped_files))
    # print(len(filelist))
    # print(datelist)
    # print(sum(datelist))
    return filelist


#clean data and return dataframe
def clean_data(path):
    print(path)
    df = pd.read_csv(path)
    df = df.loc[:, ["Time Stamp", "Name", "LBMP ($/MWHr)"]]

    if 'realtime' in str(path):
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
    return df

# further clean data and save as csv
def save_as_csv(df, filename):
    df["rtm"] = df["rtm"].round(decimals=4)
    df["dart"] = df["dam"] - df["rtm"]
    df["dart"] = df["dart"].round(decimals=4)
    filename = filename + ".csv"
    path = os.path.join("sample", filename)
    df.to_csv(path)





#paths of folders
da_gen = "C:/Users/tejal_q4bz1lv/Documents/DjangoProjects/NYISOzipsample/Historical Day-Ahead Market LBMP - Generator"
rt_gen = "C:/Users/tejal_q4bz1lv/Documents/DjangoProjects/NYISOzipsample/Historical Real-Time Market LBMP - Generator"
da_zonal = "C:/Users/tejal_q4bz1lv/Documents/DjangoProjects/NYISOzipsample/Historical Day-Ahead Market LBMP - Zonal"
rt_zonal = "C:/Users/tejal_q4bz1lv/Documents/DjangoProjects/NYISOzipsample/Historical Real-Time Market LBMP - Zonal"

#get the csv files on these paths
da_gen_paths = get_csvfile_path(da_gen)
rt_gen_paths = get_csvfile_path(rt_gen)
da_zonal_paths = get_csvfile_path(da_zonal)
rt_zonal_paths = get_csvfile_path(rt_zonal)

#getting files of same dates and processing them

#zonal files
if len(da_zonal_paths) == len(rt_zonal_paths)==len(da_gen_paths) == len(rt_gen_paths):
    for i in range(len(da_zonal_paths)):
        if str(da_zonal_paths[i])[26:34] == str(rt_zonal_paths[i])[26:34] and str(da_gen_paths[i])[26:34] == str(rt_gen_paths[i])[26:34]:
            filename = str(da_zonal_paths[i])[26:34]
            da_zonal_df = clean_data(da_zonal_paths[i])
            rt_zonal_df = clean_data(rt_zonal_paths[i])
            da_gen_df = clean_data(da_gen_paths[i])
            rt_gen_df = clean_data(rt_gen_paths[i])

            #merge zonal dataframes & generator dataframes respectively
            df_zonal = pd.merge(da_zonal_df, rt_zonal_df, left_on=["fordate", "node"], right_on=["fordate","node"])
            print(df_zonal)
            df_gen = pd.merge(da_gen_df, rt_gen_df, left_on=["fordate", "node"], right_on=["fordate","node"])
            print(df_gen)
            #concat & save
            df = pd.concat([df_zonal, df_gen])
            print(df)
            df = df.sort_values(by=['fordate','node'])
            save_as_csv(df, filename)
            break
            
