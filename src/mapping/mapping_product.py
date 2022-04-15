# necessary
import os
import sys
import ast
import pickle
from tqdm.auto import tqdm

import numpy as np
import pandas as pd

from PyQt5 import QtCore

# path setting
cur_dir = os.path.dirname(os.path.realpath(__file__))
root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
src = os.path.abspath(os.path.join(cur_dir, os.pardir))
sys.path.append(root)
sys.path.append(src)

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    base_path = sys._MEIPASS
    tbl_cache = os.path.join(base_path, 'tbl_cache_')
    
else:
    base_path = os.path.dirname(os.path.realpath(__file__))
    tbl_cache = root + '/tbl_cache'
    
from hangle import _distance

# def title_comparison(word_0: str, word_1: str) -> tuple:
    
#     ''' 
#     Compare Product Titles 
#     '''
    
#     non_sp_0 = word_0.replace(' ', '')
#     non_sp_1 = word_1.replace(' ', '')

#     len_0 = len(non_sp_0)
#     len_1 = len(non_sp_1)
#     max_len = max(len_0, len_1)
#     min_len = min(len_0, len_1)
    
#     # 토큰 중복 비율 계산
#     word_sp_0 = word_0.split(' ')
#     word_sp_1 = word_1.split(' ')
    
#     '''string 종속 여부 체크'''

#     # 완전일치
#     if non_sp_0 == non_sp_1:
#         dep = 1
#         sim = 1
#         dep_ratio = 1
#         dep_cnt = len(word_sp_0) 
        
#     # string 완전 종속
#     elif non_sp_0 in non_sp_1 or non_sp_1 in non_sp_0:
#         dep = 1
#         # calculate similarity
#         sim = jaro_distance(non_sp_0, non_sp_1)
#         dep_ratio = 1
#         dep_cnt = min(len(word_sp_0), len(word_sp_1)) 
        
#     # 토큰 종속여부 체크 및 similarity 계산 
#     else:
#         leng = min(len(word_sp_0), len(word_sp_1))
#         word_set = list(set(word_sp_0) & set(word_sp_1))
#         dep_cnt = len(word_set)
#         dep_ratio = dep_cnt / leng
        
#         dep = 0
#         # calculate similarity
#         cost = jamo_levenshtein(non_sp_0, non_sp_1)
#         sim = (max_len - cost) / max_len

#     return round(sim, 4), dep, max_len, min_len, round(dep_ratio, 4), dep_cnt


# def prd_mapper(input_data_0: pd.DataFrame, input_data_1: pd.DataFrame) -> pd.DataFrame:
    
#     '''  
#     Compare Product Titles after Grouping Brands and Categories
    
#     Input Data 
#     - input_data_0: Mapping Criteria Table
#     - input_data_1: Mapping Target Table
    
#     ** necessary columns: ['id', 'brand_name', 'product_name', 'category']
    
#     Output Data 
#     - compared_df: Product name comparison table 
    
#     '''
    
    
#     # Required column for product name comparison
#     columns_necessary = ['id', 'brand_name', 'product_name', 'category', 'table_name']
#     df_notnull_0 = input_data_0[input_data_0.product_name.notnull()].loc[:, columns_necessary].reset_index(drop=True)
#     df_notnull_0.loc[:, 'brand_name'] = df_notnull_0.brand_name.str.replace(' ', '').str.lower()
#     df_notnull_1 = input_data_1[input_data_1.product_name.notnull()].loc[:, columns_necessary].reset_index(drop=True)
#     df_notnull_1.loc[:, 'brand_name'] = df_notnull_1.brand_name.str.replace(' ', '').str.lower()
    
#     brands = df_notnull_1.brand_name.unique()
#     categories = df_notnull_1.category.unique()

#     columns = ['id_1', 'id_0', 'title_1', 'title_0', 'brand_name', 'table_name', 'category_1', 'category_0', 'similarity', 'dependency', 'max_length', 'min_length', 'dependency_ratio', 'dependency_count']
#     compared_df = pd.DataFrame(columns=columns)

#     # group by brand_name
#     brd_grp_0 = df_notnull_0.groupby('brand_name')
#     brd_grp_1 = df_notnull_1.groupby('brand_name')
#     for brand in tqdm(brands):
#         df_brd_1 = brd_grp_1.get_group(brand).reset_index(drop=True)
#         try:
#             df_brd_0 = brd_grp_0.get_group(brand).reset_index(drop=True)
    
#         # 매핑 기준 테이블에 해당 브랜드가 존재하지 않는 경우 
#         except KeyError:
#             continue
        
#         for idx_1 in tqdm(range(len(df_brd_1))):
#             id_1, categ_1, title_1, tbl = df_brd_1.loc[idx_1, ['id', 'category', 'product_name', 'table_name']]

#             # brand grouping
#             df = pd.DataFrame(columns=columns)
#             ck = 0
#             for idx_0 in range(len(df_brd_0)):
#                 id_0, categ_0, title_0 = df_brd_0.loc[idx_0, ['id', 'category', 'product_name']]

#                 # category grouping
#                 if categ_1 == categ_0:
#                     # product name comparing
#                     compare_output = title_comparison(title_1, title_0)
#                     sim = compare_output[0]

#                     # similarity = 1 -> break
#                     if sim == 1:
#                         compared_df.loc[len(compared_df)] = (id_1, id_0, title_1, title_0, brand, tbl, categ_1, categ_0) + compare_output
#                         ck = 1
#                         break

#                     else:
#                         df.loc[len(df)] = (id_1, id_0, title_1, title_0, brand, tbl, categ_1, categ_0) + compare_output

#                 else:
#                     pass

#             # data assigning to DataFrame
#             if ck == 1:
#                 pass

#             elif len(df) == 0:
#                 pass

#             else:
#                 compared_df = pd.concat([compared_df, df])
    
#     compared_df = compared_df.reset_index(drop=True)
                
#     return compared_df


# def select_mapped_prd(input_data: pd.DataFrame) -> pd.DataFrame:
    
#     '''  
#     Select Mapped Products by Criteria You Define
    
#     Input Data 
#     - input_data: Product name comparison table
    
#     ** necessary columns = ['id_1', 'id_0', 'title_1', 'title_0', 'brand_name', 'table_name', 'category_1', 'category_0', 'similarity', 'dependency', 'max_length', 'min_length', 'dependency_ratio', 'dependency_count']
    
#     Output Data 
#     - mapped_df: Product Name Mapping Complete Table
    
#     '''
    
#     params = {
#         'min_length': 6, # product name minimum length
#         'min_token': 3, # product name token minimum length
#         'levenshtein_similarity': round(5/6, 4) # similarity minimum value
#     }

#     min_len = params['min_length']
#     min_tk = params['min_token']
#     min_sim = params['levenshtein_similarity']
    
#     compared_df = input_data.copy()
#     # mapping group 0: simimlarity == 1 & min_length >= min_len
#     grp_0 = compared_df[(compared_df.similarity==1) & (compared_df.min_length>=min_len)]
#     grp_sim = grp_0.sort_values(by=['id_1', 'dependency_ratio', 'dependency_count', 'similarity'], ascending=False).reset_index(drop=True)

#     # mapping group 1: simimlarity != 1 & dependency_ratio = 1 & min_length >= min_len
#     grp_1 = compared_df[(compared_df.similarity!=1) & (compared_df.dependency_ratio==1) & (compared_df.min_length>=min_len)].sort_values(by=['id_1', 'dependency_count', 'similarity'], ascending=False)
#     grp_dedup = grp_1.drop_duplicates(subset=['id_1'], keep='first').reset_index(drop=True)

#     # mapping group 2: simimlarity != 1 & dependency_ratio != 1 & min_length >= min_len
#     df_dedup = pd.concat([compared_df, grp_sim, grp_dedup]).drop_duplicates(subset=['id_1'], keep=False).reset_index(drop=True)
#     grp_2 = df_dedup[df_dedup.min_length>=min_len].reset_index(drop=True)

#     # calculate levenshtein distance
#     for idx in tqdm(range(len(grp_2))):
#         s0 = grp_2.loc[idx, 'title_0'].replace(' ', '')
#         s1 = grp_2.loc[idx, 'title_1'].replace(' ', '')
#         sim = grp_2.loc[idx, 'similarity']
#         max_len = grp_2.loc[idx, 'max_length']
#         winkler = jaro_winkler(s0, s1)
#         cost = jamo_levenshtein(s0, s1)
#         levenshtein_similarity = (max_len - cost) / max_len
#         grp_2.loc[idx, ['jaro_winker', 'levenshtein_distance', 'levenshtein_similarity']] = round(winkler, 4), float(cost), round(levenshtein_similarity, 4)
    
#     # mapping group 3: simimlarity != 1 & dependency_ratio != 1 & min_length >= min_len & levenshtein_similarity >= min_sim
#     grp_levenshtein = grp_2.loc[(grp_2.levenshtein_similarity>=min_sim)].reset_index(drop=True)

#     mapped_df = pd.concat([grp_sim, grp_dedup, grp_levenshtein]).sort_values('id_1').reset_index(drop=True)

#     return mapped_df


def title_comparison(word_0: str, word_1: str) -> tuple:
    
    ''' 
    Compare Product Titles 
    '''
    
    non_sp_0 = word_0.replace(' ', '')
    non_sp_1 = word_1.replace(' ', '')

    len_0 = len(non_sp_0)
    len_1 = len(non_sp_1)
    max_len = max(len_0, len_1)
    min_len = min(len_0, len_1)
    
    if non_sp_0 == non_sp_1:
        dep_ratio = np.nan
        dep_cnt = np.nan
        sim = 1
        
    else:
        # 토큰 중복 비율 계산
        word_sp_0 = word_0.split(' ')
        word_sp_1 = word_1.split(' ')
        
        leng = min(len(word_sp_0), len(word_sp_1))
        word_set = list(set(word_sp_0) & set(word_sp_1))
        dep_cnt = len(word_set)
        dep_ratio = dep_cnt / leng

        # calculate similarity
        cost = _distance.jamo_levenshtein(non_sp_0, non_sp_1)
        sim = (max_len - cost) / max_len

    return round(dep_ratio, 4), dep_cnt, round(sim, 4), max_len, min_len

def map_expand(mapping_table: pd.DataFrame) -> pd.DataFrame:
    mapping_table = mapping_table.reset_index(drop=True)
    
    map_list = []
    for idx in tqdm(range(len(mapping_table))):
        id_0 = mapping_table.iloc[idx, 0]
        ids = mapping_table.iloc[idx, 1]
        tbl = mapping_table.iloc[idx, 2]
        
        if ids[0] == '[':
            ids = ast.literal_eval(ids)
            for id_1 in ids:
                map_list.append([int(id_0), int(id_1), str(tbl)])
                # mapping_.loc[len(mapping_)] = int(id_0), int(id_1), str(tbl)

        else:
            id_1 = ids
            map_list.append([int(id_0), int(id_1), str(tbl)])
            # mapping_.loc[len(mapping_)] = int(id_0), int(id_1), str(tbl)
    
    mapping_ = pd.DataFrame(map_list, columns=['item_key', 'id', 'table_name'])
    return mapping_

def _prd_mapper():
    
    df_0 = pd.read_csv(tbl_cache + '/deprepro_0.csv')
    df_1 = pd.read_csv(tbl_cache + '/deprepro_1.csv')

    # concat, drop category null
    df_concat = pd.concat([df_0, df_1])
    df_concat_ = df_concat[df_concat.category.notnull()].reset_index(drop=True)
    # edit brand, title
    df_concat_.loc[:, 'brand_name_'] = df_concat_.brand_name.str.replace(' ', '').str.lower()
    df_concat_.loc[:, 'product_name_'] = df_concat_.product_name.str.replace(' ', '')
    # length condition check
    df_concat_.loc[:, 'length'] = df_concat_.product_name_.str.len()
    df_len = df_concat_[df_concat_.length >= 6].reset_index(drop=True)

    # find duplicate
    subset = ['brand_name_', 'category', 'product_name_']
    df_dup = df_len[df_len.duplicated(subset=subset, keep=False)]

    # grouping 
    df_grp = df_dup.groupby(subset)
    grp_index = df_grp.size().index

    # 브랜드, 카테고리, 상품명 모두 일치하는 상품들 그룹핑 후 매핑 
    map_list, map_list_ = [], []
    for idx in tqdm(grp_index):
        # 하나의 (brand_name, category, product_name) 유니크 그룹
        df = df_grp.get_group(idx).reset_index(drop=True)

        # 글로우픽 기준으로 매핑 작업 
        if 'glowpick_product_info_final_version' in df.table_name.tolist():
            id_ = df.loc[df.table_name=='glowpick_product_info_final_version'].id.tolist()[0]  
            title = df.loc[0, 'product_name']
            brand = df.loc[0, 'brand_name']
            categ = df.loc[0, 'category']
            length = df.loc[0, 'length']

            tbls = df.loc[df.table_name!='glowpick_product_info_final_version'].table_name.unique().tolist()
            for tbl in tbls:
                mapped_ids = df.loc[df.table_name==tbl].id.tolist()

                if len(mapped_ids) == 0:
                    continue

                elif len(mapped_ids) == 1:
                    mapped_id = int(mapped_ids[0])
                    map_list.append([int(mapped_id), int(id_), str(title), str(title),  str(brand), str(categ), str(tbl), np.nan, np.nan, 1, length, length])

                    mapped_id = str(mapped_ids[0])
                else:
                    for mapped_id in mapped_ids:
                        map_list.append([int(mapped_id), int(id_), str(title), str(title),  str(brand), str(categ), str(tbl), np.nan, np.nan, 1, length, length])

                    mapped_id = str(mapped_ids)

                map_list_.append([int(id_), str(mapped_id), str(tbl)])

        else:
            pass

    columns=['glowpick_product_info_final_version_id', 'mapped_id', 'table_name'] 
    df_map = pd.DataFrame(map_list_, columns=columns)

    # 전체 데이터에서 매핑된 데이터 제거 (차집합 구하기)
    df_map_ = map_expand(df_map)
    subset = ['id', 'table_name']
    df_concat = pd.concat([df_1, df_map_.loc[:, subset]])
    df_dedup = df_concat.drop_duplicates(subset=subset, keep=False).reset_index(drop=True)
    
    return df_dedup, map_list

def prd_mapper():
    
    '''  
    Compare Product Titles after Grouping Brands and Categories
    
    Input Data 
    - input_data_0: Mapping Criteria Table
    - input_data_1: Mapping Target Table
    
    ** necessary columns: ['id', 'brand_name', 'product_name', 'category']
    
    Output Data 
    - compared_df: Product name comparison table 
    
    '''
    
    outputs = _prd_mapper()
    input_data_0 = pd.read_csv(tbl_cache + '/deprepro_0.csv')
    input_data_1 = outputs[0]
    map_list = outputs[1]
    
    # 상품명 NaN값 드랍하기 및 브랜드명 전처리 
    df_notnull_0 = input_data_0[input_data_0.product_name.notnull()].reset_index(drop=True)
    df_notnull_0.loc[:, 'brand_name'] = df_notnull_0.brand_name.str.replace(' ', '').str.lower()
    df_notnull_1 = input_data_1[input_data_1.product_name.notnull()].reset_index(drop=True)
    df_notnull_1.loc[:, 'brand_name'] = df_notnull_1.brand_name.str.replace(' ', '').str.lower()
    
    # group by brand_name
    brd_grp_0 = df_notnull_0.groupby('brand_name')
    brd_grp_1 = df_notnull_1.groupby('brand_name')
    brands = brd_grp_1.size().index.tolist()
    compared_list = []
    for brand in tqdm(brands):
        df_brd_1 = brd_grp_1.get_group(brand).reset_index(drop=True)
        
        try:
            df_brd_0 = brd_grp_0.get_group(brand).reset_index(drop=True)
    
        except KeyError: # 매핑 기준 테이블에 해당 브랜드가 존재하지 않는 경우 
            continue
        
        # group by category
        categ_grp_0 = df_brd_0.groupby('category')
        categ_grp_1 = df_brd_1.groupby('category')
        categs = categ_grp_1.size().index.tolist()
        for categ in categs:
            df_categ_1 = categ_grp_1.get_group(categ).reset_index(drop=True)
            
            try:
                df_categ_0 = categ_grp_0.get_group(categ).reset_index(drop=True)
                
            except KeyError: # 매핑 기준 테이블에 해당 카테고리가 존재하지 않는 경우 
                continue
            
            for idx_1 in range(len(df_categ_1)):
                id_1, title_1, tbl = df_categ_1.loc[idx_1, ['id', 'product_name', 'table_name']]
                
                
                for idx_0 in range(len(df_categ_0)):
                    id_0, title_0 = df_categ_0.loc[idx_0, ['id', 'product_name']]
                    
                    compare_output = title_comparison(title_0, title_1)
                    compared_list.append(list((id_1, id_0, title_1, title_0, brand, categ, tbl) + compare_output))
                    
    compared_list_ = map_list + compared_list                         
    with open(tbl_cache + '/compared_list.txt', 'wb') as f:
            pickle.dump(compared_list_, f)
            
    columns = ['id_1', 'id_0', 'title_1', 'title_0', 'brand_name', 'category', 'table_name', 'dependency_ratio', 'dependency_count', 'similarity', 'max_length', 'min_length']
    compared_df = pd.DataFrame(compared_list_, columns=columns).reset_index(drop=True)
    
    return compared_df


def select_mapped_prd(input_data: pd.DataFrame) -> pd.DataFrame:
    
    '''  
    Select Mapped Products by Criteria You Define
    
    Input Data 
    - input_data: Product name comparison table
    
    ** necessary columns = ['id_1', 'table_name', 'dependency_ratio', 'dependency_count', 'similarity', 'max_length', 'min_length']
    
    Output Data 
    - mapped_df: Product Name Mapping Complete Table
    
    '''
    compared_df = input_data.copy()

    ''' 여기서 매핑 기준 파라미터 값을 변경할 수 있습니다 '''
    params = {
        'min_length': 6, # product name minimum length
        'min_token': 3, # product name token minimum length
        'levenshtein_similarity': round(5/6, 4) # similarity minimum value
    }

    min_len = params['min_length']
    min_tk = params['min_token']
    min_sim = params['levenshtein_similarity']

    # mapping group 0: simimlarity == 1 & min_length >= min_len
    grp_0 = compared_df[(compared_df.similarity==1) & (compared_df.min_length>=min_len)]
    grp_sim = grp_0.drop_duplicates(subset=['id_1', 'table_name'], keep='first').reset_index(drop=True)

    # mapping group 1: simimlarity != 1 & dependency_ratio = 1 & min_length >= min_len
    grp_1 = compared_df[(compared_df.similarity!=1) & (compared_df.dependency_ratio==1) & (compared_df.min_length>=min_len)]
    grp_dep = grp_1.sort_values(by=['dependency_count', 'similarity'], ascending=False).drop_duplicates(subset=['id_1', 'table_name'], keep='first').reset_index(drop=True)

    # mapping group 2: simimlarity != 1 & dependency_ratio != 1 & min_length >= min_len & levenshtein_similarity >= min_sim
    grp_2 = compared_df[(compared_df.similarity!=1) & (compared_df.dependency_ratio!=1) & (compared_df.min_length>=min_len) & (compared_df.similarity>=min_sim)]
    grp_levenshtein = grp_2.sort_values(by=['similarity', 'dependency_count'], ascending=False).drop_duplicates(subset=['id_1', 'table_name'], keep='first').reset_index(drop=True)

    # concat & dup check
    df_concat = pd.concat([grp_sim, grp_dep, grp_levenshtein])
    mapped_df = df_concat.sort_values(by=['dependency_ratio', 'dependency_count', 'similarity']).drop_duplicates(subset=['id_1', 'table_name'], keep='first').reset_index(drop=True)

    return mapped_df


def md_map_tbl(input_data: pd.DataFrame) -> pd.DataFrame:
    ''' Creating a mapping table '''

    tables = input_data.table_name.unique()
    map_list = []
    for tbl in tables:
        df = input_data.loc[input_data.table_name==tbl].reset_index(drop=True)

        for id_0 in tqdm(df.id_0.unique()):
                ids = df.loc[df.id_0==id_0, 'id_1'].values.tolist()

                if len(ids) == 1:
                    ids = str(ids[0])

                else:
                    ids = str(ids)
                map_list.append([int(id_0), ids, tbl])
    
    # assign
    mapping_table = pd.DataFrame(map_list, columns=['glowpick_product_info_final_version_id', 'mapped_id', 'table_name'])
    
    # sort
    mapping_table = mapping_table.sort_values(by=['glowpick_product_info_final_version_id', 'table_name']).reset_index(drop=True)
    
    return mapping_table



class ThreadComparing(QtCore.QThread, QtCore.QObject):
    ''' Thread comparing product info '''
    
    def __init__(self, parent=None):
        super().__init__()
        self.power = True
        
    progress = QtCore.pyqtSignal(object)
    def run(self):
        ''' Run Thread '''
        
        outputs = _prd_mapper()
        input_data_0 = pd.read_csv(tbl_cache + '/deprepro_0.csv')
        input_data_1 = outputs[0]
        map_list = outputs[1]
        
        # 상품명 NaN값 드랍하기 및 브랜드명 전처리 
        df_notnull_0 = input_data_0[input_data_0.product_name.notnull()].reset_index(drop=True)
        df_notnull_0.loc[:, 'brand_name'] = df_notnull_0.brand_name.str.replace(' ', '').str.lower()
        df_notnull_1 = input_data_1[input_data_1.product_name.notnull()].reset_index(drop=True)
        df_notnull_1.loc[:, 'brand_name'] = df_notnull_1.brand_name.str.replace(' ', '').str.lower()
        
        compared_list = []

        # group by brand_name
        brd_grp_0 = df_notnull_0.groupby('brand_name')
        brd_grp_1 = df_notnull_1.groupby('brand_name')
        brands = brd_grp_1.size().index.tolist()
        
        cnt = 0
        t = tqdm(brands)
        for brand in t:
            if self.power == True:
                self.progress.emit(t)
                cnt += 1
                
                df_brd_1 = brd_grp_1.get_group(brand).reset_index(drop=True)
                
                try:
                    df_brd_0 = brd_grp_0.get_group(brand).reset_index(drop=True)
            
                except KeyError: # 매핑 기준 테이블에 해당 브랜드가 존재하지 않는 경우 
                    continue
                
                # group by category
                categ_grp_0 = df_brd_0.groupby('category')
                categ_grp_1 = df_brd_1.groupby('category')
                categs = categ_grp_1.size().index.tolist()
                for categ in categs:
                    df_categ_1 = categ_grp_1.get_group(categ).reset_index(drop=True)
                    
                    try:
                        df_categ_0 = categ_grp_0.get_group(categ).reset_index(drop=True)


                    except KeyError: # 매핑 기준 테이블에 해당 카테고리가 존재하지 않는 경우 
                        continue
                    
                    
                    for idx_1 in range(len(df_categ_1)):
                        id_1, title_1, tbl = df_categ_1.loc[idx_1, ['id', 'product_name', 'table_name']]
                        
                        
                        for idx_0 in range(len(df_categ_0)):
                            id_0, title_0 = df_categ_0.loc[idx_0, ['id', 'product_name']]
                            
                            compare_output = title_comparison(title_0, title_1)
                            compared_list.append(list((id_1, id_0, title_1, title_0, brand, categ, tbl) + compare_output))
                
            else:
                self.progress.emit(t)
                break
          
        if cnt == len(brands):
            compared_list_ = map_list + compared_list                         
            with open(tbl_cache + '/compared_list.txt', 'wb') as f:
                    pickle.dump(compared_list_, f)
                    
            columns = ['id_1', 'id_0', 'title_1', 'title_0', 'brand_name', 'category', 'table_name', 'dependency_ratio', 'dependency_count', 'similarity', 'max_length', 'min_length']
            compared_df = pd.DataFrame(compared_list_, columns=columns).reset_index(drop=True)
            compared_df.to_csv(tbl_cache + '/compared_prds.csv', index=False)
        
        
    def stop(self):
        ''' Stop Thread '''
        
        self.power = False
        self.quit()
        self.wait(3000)





# def update_map_tbl(user_name, password, db_name):
    
#     ''' update mapping table '''
#     db = access_db.AccessDataBase(user_name, password, db_name)
    
#     # get existing mapping table to db
#     map_tbl_ex = db.get_tbl(db_name, 'naver_glowpick_mapping_table', 'all') 
#     # get new mapping table to dir(tbl_cache)
#     map_tbl_new = pd.read_csv(tbl_cache + '/mapping_table.csv')
    
        
#     df_concat = pd.concat([map_tbl_ex, map_tbl_new]).reset_index(drop=True)

#     # updated mapping table
#     map_tbl = pd.DataFrame(columns=map_tbl_new.columns)

#     for tbl in df_concat.table_name.unique():
#         df_tbl = df_concat[df_concat.table_name==tbl]
        
#         for id_ in tqdm(df_tbl.glowpick_product_info_final_version_id.unique()):
#             mapped_id = df_tbl.loc[df_tbl.glowpick_product_info_final_version_id==id_, 'mapped_id']
            
#             mapped_ids = []
#             for ids in mapped_id:
#                 if ids[0] == '[':
#                     ids = ast.literal_eval(ids)
#                     mapped_ids += ids
                
#                 else:
#                     mapped_ids.append(int(ids))
            
#             mapped_ids = list(set(mapped_ids))
#             if len(mapped_ids) == 0:
#                 print('wrn')
#                 break
            
#             elif len(mapped_ids) == 1:
#                 mapped_ids = str(mapped_ids[0])

#             else:
#                 mapped_ids = str(mapped_ids)

                
#             map_tbl.loc[len(map_tbl)] = int(id_), str(mapped_ids), str(tbl)
            
#     map_tbl = map_tbl.sort_values(by=['glowpick_product_info_final_version_id', 'table_name']).reset_index(drop=True)
#     map_tbl.to_csv(tbl_cache + '/naver_glowpick_mapping_table.csv', index=False)

#     # dup check
#     dup = map_tbl[map_tbl.duplicated(subset=['glowpick_product_info_final_version_id', 'table_name'], keep=False)]
#     if len(dup) == 0:
#         comment = 'Mapping table update complete!'
    
#     else:
#         comment = 'Duplicate check required!'
    
#     unq_prd_0 = len(map_tbl.glowpick_product_info_final_version_id.unique())
#     unq_prd_1 = (map_tbl.mapped_id.str.count(',') + 1).sum()
    
#     return comment, unq_prd_0, unq_prd_1
    
    
# def upload_map_tbl(user_name, password, db_name, table_name, columns):
    
#     ''' upload mapping table 
    
#     <업로드 조건> 
#     ** 기존 매핑 테이블 오늘 날짜로 백업 완료
#     ** db에서 매핑테이블 생성 및 컬럼명 일치 여부 확인 
     
#     '''
#     db = access_db.AccessDataBase(user_name, password, db_name)
    
#     map_tbl = pd.read_csv(tbl_cache + '/naver_glowpick_mapping_table.csv').loc[:, columns]
    
#     db.engine_upload(map_tbl, db_name, table_name)
    
#     comment = 'Mapping table upload is complete!'
    
#     return comment