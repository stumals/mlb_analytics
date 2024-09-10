

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
def get_data(year):
    df = pd.read_parquet(os.getcwd() + f'\\data\\statcast_{year}.gzip')
    return df

def get_bp_data(df):
    df['events'] = df['events'].fillna('None')
    df = df[df['events'] != 'None']
    df = df[['batter', 'pitcher', 'events']]
    df['batter'] = df['batter'].astype(str)
    df['pitcher'] = df['pitcher'].astype(str)
    return df

def get_graph_data(df):
    with open('mappings.yaml', encoding='utf-8') as f:
        mappings = yaml.safe_load(f)
    tb_dict = dict(map(lambda x: (x[0], x[1]['total_bases']), mappings.items()))
    df['total_bases'] = df['events'].map(tb_dict)
    df = df[['batter', 'pitcher', 'total_bases']]
    df['bat-pitch'] = list(zip(df['batter'], df['pitcher']))
    df2 = df[['bat-pitch', 'total_bases']].groupby('bat-pitch').sum().reset_index()
    df2['batter'], df2['pitcher'] = zip(*df2['bat-pitch'])
    df3 = df2[['batter', 'pitcher', 'total_bases']]
    return df3

def create_weighted_page_rank(df):
    graph = nx.from_pandas_edgelist(df, source='batter', target='pitcher', edge_attr='total_bases')
    weighted_pr = nx.pagerank(graph, weight='total_bases')
    pr = pd.Series(weighted_pr).sort_values(ascending=False)
    pr = pr.to_frame(name='page_rank')
    return pr

def get_mlb_ids():
    if 'mlb_ids.gzip' in os.listdir(os.getcwd() + '\\data'):
        mlb_ids = pd.read_parquet(os.getcwd() + '\\data\\mlb_ids.gzip')
    else:
        url = 'https://www.smartfantasybaseball.com/PLAYERIDMAPCSV'
        data = []
        with requests.Session() as s:
            download = s.get(url, headers={'User-Agent': 'adfcv'})

            decoded_content = download.content.decode('utf-8')

            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
            for row in my_list:
                data.append(row)
        data = pd.DataFrame(data[1:], columns=data[0])
        mlb_ids = data[['MLBID', 'MLBNAME', 'TEAM', 'POS']]
        mlb_ids.to_parquet(os.getcwd() + '\\data\\mlb_ids.gzip')
    return mlb_ids

# def map_mlb_ids(df):
#     mlb_ids = get_mlb_ids()
#     df['batter'] = df['batter'].astype(int)
#     df['pitcher'] = df['pitcher'].astype(int)
#     df = df.merge(mlb_ids, left_on='batter', right_on='MLBID', how='left')
#     df = df.merge(mlb_ids, left_on='pitcher', right_on='MLBID', how='left')
#     df = df[['MLBNAME_x', 'MLBNAME_y', 'total_bases']]
#     df.columns = ['batter', 'pitcher', 'total_bases']
#     return df

def map_mlb_ids(pr):
    mlb_ids = get_mlb_ids()
    pr = pr.reset_index()
    pr = pr.merge(mlb_ids, left_on='index', right_on='MLBID', how='left')
    #pr = pr[['MLBNAME', 'page_rank']]
    #pr.columns = ['player', 'page_rank']
    return pr


def create_batter_rankings(year):
    df = get_data(year)
    df = get_bp_data(df)
    df = get_graph_data(df)
    pr = create_weighted_page_rank(df)
    pr = map_mlb_ids(pr)
    return pr


#%%
pr = create_batter_rankings(2023)
#%%
pr.head(20)

# %%

mlb_ids = get_mlb_ids()
# %%
pr.reset_index().info()
#%%