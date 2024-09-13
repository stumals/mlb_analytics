#%%
import os
import pandas as pd
import yaml
import networkx as nx
import requests
import csv

def get_data(year : int) -> pd.DataFrame:
    df = pd.read_parquet(os.getcwd() + f'\\data\\statcast_{year}.gzip')
    return df

def get_bp_data(df: pd.DataFrame) -> pd.DataFrame:
    df['events'] = df['events'].fillna('None')
    df = df[df['events'] != 'None']
    df = df[['batter', 'pitcher', 'events']]
    df['batter'] = df['batter'].astype(str)
    df['pitcher'] = df['pitcher'].astype(str)
    return df

def get_graph_data(df : pd.DataFrame) -> pd.DataFrame:
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

def create_weighted_page_rank(df : pd.DataFrame) -> pd.DataFrame:
    graph = nx.from_pandas_edgelist(df, source='batter', target='pitcher', edge_attr='total_bases')
    weighted_pr = nx.pagerank(graph, weight='total_bases')
    pr = pd.Series(weighted_pr).sort_values(ascending=False)
    pr = pr.to_frame(name='page_rank')
    return pr

def get_mlb_ids() -> pd.DataFrame:
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

def map_mlb_ids(pr : pd.DataFrame) -> pd.DataFrame:
    mlb_ids = get_mlb_ids()
    pr = pr.reset_index()
    pr = pr.merge(mlb_ids, left_on='index', right_on='MLBID', how='left')
    pr = pr[['MLBID', 'MLBNAME', 'TEAM', 'POS', 'page_rank']]
    pr  = pr.drop_duplicates()
    pr = pr[pr['POS'] != 'P']
    return pr


def create_batter_rankings(year : int) -> pd.DataFrame:
    df = get_data(year)
    df = get_bp_data(df)
    df = get_graph_data(df)
    pr = create_weighted_page_rank(df)
    pr = map_mlb_ids(pr)
    return pr

# %%
create_batter_rankings(2023).head(20)