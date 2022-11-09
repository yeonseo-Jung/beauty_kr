import os
import re
import sys
import ast

import numpy as np
import pandas as pd
from tqdm import tqdm

# Scrapping
from bs4 import BeautifulSoup

# # Exception Error Handling
# import socket
# import warnings
# warnings.filterwarnings("ignore")

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src = os.path.join(root, 'src')
    sys.path.append(src)

tbl_cache = os.path.join(root, 'tbl_cache')
conn_path = os.path.join(root, 'conn.txt')
    
from scraping.scraper import get_url
from access_database.access_db import AccessDataBase
from mapping._preprocessing import TitlePreProcess

user_name = "yeonseosla"
password = "jys9807"
db_name = "beauty_kr"
db = AccessDataBase(user_name, password, db_name)
tp = TitlePreProcess()

def get_ingredients():
    ''' Get data all product ingredients '''
    
    df = db.get_tbl('glowpick_product_info_final_version')
    df_ingrd = df.loc[:, ['id', 'ingredients_all_kor', 'ingredients_all_eng', 'ingredients_all_desc']]
    df_ingrd = df_ingrd[df_ingrd.ingredients_all_kor.notnull()].reset_index(drop=True)
    _ingrds_, error = [], []
    for i in tqdm(range(len(df_ingrd))):
        ingrds = df_ingrd.iloc[i, 1:].tolist()
        ingrds_ko = ast.literal_eval(ingrds[0])

        _ingrds = []
        for j in range(len(ingrds_ko)):
            
            try:
                # ko
                ko = ast.literal_eval(ingrds[0])[j]
                
                # en
                if ingrds[1] == '[nan]':
                    en = 'nan'
                else:
                    ingrds_en = ingrds[1].replace('nan,', "'nan',").replace(' nan', " 'nan'")
                    en = ast.literal_eval(ingrds_en)[j] 
                
                # desc
                if ingrds[1] == '[nan]':
                    desc = 'nan'
                else:
                    ingrds_desc = ingrds[2].replace('nan,', "'nan',").replace(' nan', " 'nan'")
                    desc = ast.literal_eval(ingrds_desc)[j]
                
                # check nan
                if en == 'nan':
                    en = np.nan
                if desc == 'nan':
                    desc = np.nan
                else:
                    desc = str(desc)
                _ingrds.append([ko, en, desc])
                
            except ValueError:
                error.append(ingrds)
        _ingrds_ += _ingrds
    ingredients_df = pd.DataFrame(_ingrds_, columns=['ingredient_ko', 'ingredient_en', 'ingredient_desc']).sort_values(by=['ingredient_en', 'ingredient_desc'])
    ingredients_df_dedup = ingredients_df[ingredients_df.ingredient_en.notnull()].drop_duplicates('ingredient_en', keep='first', ignore_index=True)
    
    return ingredients_df_dedup


def scraping_ewg(url):
    ''' Scraping ewg ingredients data '''
    
    wd = get_url(url)
    soup = BeautifulSoup(wd.page_source, 'lxml')
    
    # scraping ingredient data
    ingrds = soup.find_all('div', 'product-tile')
    ingrds_data = []
    for ingrd in ingrds:
        ingrd_url = ingrd.find('div', 'product-image-wrapper flex').find('a')['href']
        ewg_url = 'https://www.ewg.org' 
        ingrd_url = ewg_url + ingrd_url

        ingrd_name = ingrd.find('div', 'product-name').text
        score = ingrd.find('div', 'product-score')

        score_img_src = score.find('img')['src']
        score_img_src = ewg_url + score_img_src
        score_data = int(re.search(r'score=[0-9]+', score_img_src).group(0)[-1])

        score_availability = score.find('div', 'data-level').text.replace('Data Availability:', '').strip()
        
        ingrds_data.append([ingrd_url, ingrd_name, score_data, score_availability, score_img_src])
        
    wd.quit()
        
    return ingrds_data

def crawling_ewg():
    ''' Crawling ewg ingredients data '''
    
    ingredients_df_dedup = get_ingredients()
    ingredients = ingredients_df_dedup.ingredient_en.tolist()
    ewgs, over = [], []
    for ingredient in tqdm(ingredients):
        _ingredient = ingredient.replace(' ', '+')
        url = f'https://www.ewg.org/skindeep/search/?search={_ingredient}&search_type=ingredients'
        try:
            ewg = scraping_ewg(url)
            ewgs += ewg
            
            if len(ewg) == 12:
                over.append(url)
                
        except ConnectionError as e:
            print(e)
            
    return ewgs, ingredients_df_dedup

def create_ewg():
    ''' Create ewg ingredients table '''
    
    ewgs, ingredients_df_dedup = crawling_ewg()
    ewg_df = pd.DataFrame(ewgs, columns=['ewg_url', 'ewg_ingredient_name', 'ewg_score', 'availability', 'score_img_src'])
    ewg_df_dedup = ewg_df.drop_duplicates(subset=['ewg_ingredient_name', 'availability', 'ewg_score'], ignore_index=True)

    # upload ewg ingredients table
    # db.engine_upload(ewg_df_dedup, 'ewg_ingredients_all', 'replace')
    
    ## test
    db.engine_upload(ewg_df_dedup, 'ewg_ingredients_all_test', 'replace')

    # lower & replace space
    ewg_df_dedup.loc[:, 'name'] = ewg_df_dedup.ewg_ingredient_name.str.lower().str.replace(' ', '')
    ewg_df_dedup = ewg_df_dedup.sort_values(by=['ewg_url', 'name']).drop_duplicates(subset='name', keep='last', ignore_index=True)

    ingredients_df_dedup.loc[:, 'name'] = ingredients_df_dedup.ingredient_en.str.lower().str.replace(' ', '')

    # merge on en ingredient name (left inner join)
    ingredients_merge_df = ingredients_df_dedup.merge(ewg_df_dedup, on='name', how='left').drop(columns='name')
    print(f'* 전체 성분 수:{len(ingredients_merge_df)}\n* EWG 성분 수: {len(ewg_df_dedup)}\n* 매핑 완료 성분 수: {len(ingredients_merge_df[ingredients_merge_df.ewg_url.notnull()])}')

    # init table & upload ingredients all table
    db.create_table(upload_df=ingredients_merge_df, table_name='beauty_kr_ingredients_all')
    
    ## test
    # db.engine_upload(upload_df=ingredients_merge_df, table_name='beauty_kr_ingredients_all_test', if_exists_option='replace')

    # # update table
    # db.create_table(upload_df=ingredients_merge_df, table_name='beauty_kr_ingredients_all', append=True)
    
    
def mapping_ingredients():    
    ''' Mapping ingredients '''
    
    df_ingrds = db.get_tbl('beauty_kr_ingredients_all')
    df_gl = db.get_tbl('glowpick_product_info_final_version')

    df_ingrd = df_gl.loc[df_gl.ingredients_all_eng.notnull(), ['id', 'ingredients_all_kor', 'ingredients_all_eng', 'ingredients_all_desc']].reset_index(drop=True)
    _ingrds_, error = [], []
    for i in tqdm(range(len(df_ingrd))):
        item_key = df_ingrd.loc[i, 'id']
        ingrds = df_ingrd.iloc[i, 1:].tolist()
        ingrds_ko = ast.literal_eval(ingrds[0])

        _ingrds = []
        for j in range(len(ingrds_ko)):
            
            try:
                # ko
                ko = ast.literal_eval(ingrds[0])[j]
                
                # en
                if ingrds[1] == '[nan]':
                    en = 'nan'
                else:
                    ingrds_en = ingrds[1].replace('nan,', "'nan',").replace(' nan', " 'nan'")
                    en = ast.literal_eval(ingrds_en)[j] 
                
                # desc
                if ingrds[1] == '[nan]':
                    desc = 'nan'
                else:
                    ingrds_desc = ingrds[2].replace('nan,', "'nan',").replace(' nan', " 'nan'")
                    desc = ast.literal_eval(ingrds_desc)[j]
                
                # check nan
                if en == 'nan':
                    en = np.nan
                if desc == 'nan':
                    desc = np.nan
                else:
                    desc = str(desc)
                _ingrds.append([item_key, ko, en, desc])
                
            except ValueError:
                error.append(ingrds)
        _ingrds_ += _ingrds
    _df_ingrd = pd.DataFrame(_ingrds_, columns=df_ingrd.columns)
    _df_ingrd_notnull = _df_ingrd[_df_ingrd.ingredients_all_eng.notnull()].reset_index(drop=True)

    _df_ingrd_notnull.loc[:, 'name'] = _df_ingrd_notnull.ingredients_all_eng.str.lower().str.replace(' ', '')
    df_ingrds.loc[:, 'name'] = df_ingrds.ingredient_en.str.lower().str.replace(' ', '')
    df_mer = _df_ingrd_notnull.loc[:, ['id', 'name']].merge(df_ingrds.loc[:, ['ingredient_key', 'name']], on='name', how='left').drop(columns='name').rename(columns={'id': 'item_key'})
    df_mer_sorted = df_mer.sort_values(by=df_mer.columns.tolist(), ignore_index=True)

    # init table & upload ingredients bridge table
    db.create_table(upload_df=df_mer_sorted, table_name='beauty_kr_ingredients_bridge_table')

    ## test
    # db.engine_upload(upload_df=df_mer, table_name='beauty_kr_ingredients_bridge_table_test', if_exists_option='replace')
    
if __name__ == '__main__':
    
    create_ewg()
    mapping_ingredients()