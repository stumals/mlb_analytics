

#%%
import pandas as pd
import polars as pl
import networkx as nx
import requests
import csv
import os
import yaml
import matplotlib.pyplot as plt
from pybaseball import statcast, batting_stats, playerid_reverse_lookup
#%%

def get_statcast_data(year):
    dates = [(f"{year}-03-28", f"{year}-04-30"),
            (f"{year}-05-01", f"{year}-05-31"),
            (f"{year}-06-01", f"{year}-06-30"),
            (f"{year}-07-01", f"{year}-07-31"),
            (f"{year}-08-01", f"{year}-08-31"),
            (f"{year}-09-01", f"{year}-10-01"),
            (f'{year}-10-02', f'{year}-11-30')]
    df = pd.DataFrame()
    for date in dates:
        df_new = statcast(start_dt=date[0], end_dt=date[1])
        df = pd.concat([df, df_new])
    return df

#%%
df_2019 = pd.read_parquet(os.getcwd() + '\\data\\statcast_2019.gzip')
df_2020 = pd.read_parquet(os.getcwd() + '\\data\\statcast_2020.gzip')
df_2021 = pd.read_parquet(os.getcwd() + '\\data\\statcast_2021.gzip')
df_2022 = pd.read_parquet(os.getcwd() + '\\data\\statcast_2022.gzip')
#%%
df = pd.read_parquet(os.getcwd() + '\\data\\statcast_2023.gzip')
# %%
