import os
import re
import sys
import ast
import time
import pickle
from tqdm.auto import tqdm
from itertools import chain, repeat

import numpy as np
import pandas as pd

cur_dir = os.path.dirname(os.path.realpath(__file__))
root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
src = os.path.abspath(os.path.join(cur_dir, os.pardir))
tbl_cache = root + '/tbl_cache'
sys.path.append(root)
sys.path.append(src)
sys.path.append(tbl_cache)

from PyQt5 import uic
from PyQt5 import QtCore
from PyQt5.QtWidgets import *

from access_database import access_db


# # 추출해야 할 단어 정규식 표현
# volume_ml = re.compile('[x]*\s*[0-9]*[.]?[0-9]+\s*[m]*[l]+')
# volume_kg = re.compile('[x]*\s*[0-9]*[.]?[0-9]+\s*[mk]*[g]+')
# volume_oz = re.compile('[x]*\s*[0-9]*[.]?[0-9]+\s*[fl]*\s*[ozounceounce온스]+')
# extract_reg = [volume_ml, volume_kg, volume_oz]

# # 유지해야 할 단어 정규식 
# num_0 = re.compile('[a-z]*\s*[0-9]+\s*호')
# num_1 = re.compile('#\s*[a-z]*\s*[0-9]+')
# num_2 = re.compile('[n]+[oO]+[.]?\s*[0-9]+')
# n_in_one = re.compile('[0-9]+\s?in\s?[0-9]+')
# spf = re.compile('spf\s*[0-9]*[+]*')
# pa = re.compile('pa\s*[0-9]*[+]+')
# keep_wd_reg = [num_0, num_1, num_2, n_in_one, spf, pa]
# keep_wd_list = ['num_0', 'num_1', 'num_2', 'n_in_one', 'spf', 'pa']

# # 불용어 패턴 정규식
# stp_pattern = [
# '[총x]*\s*[0-9]*[.]?[0-9]+\s*[eaEA]+\s*[0-9]*\s*[씩]?\s*[더]?',
# '[총x]*\s*[0-9]*[.]?[0-9]+\s*[매]+[입]?\s*[0-9]*\s*[씩]?\s*[더]?',
# '[총x]*\s*[0-9]*[.]?[0-9]+\s*[개]+[입]?\s*[0-9]*\s*[씩]?\s*더?',
# '[총x]*\s*[0-9]*[.]?[0-9]+\s*[팩]+\s*[0-9]*\s*[씩]?\s*[더]?',
# '[총x]*\s*[0-9]*[.]?[0-9]+\s*[장]+\s*[0-9]*\s*[씩]?\s*[더]?',
# '[총x]*\s*[0-9]*[.]?[0-9]+\s*[p]+\s*[0-9]*\s*[씩]?\s*[더]?',
# ]
# stp_pattern_reg = []
# for pattern in stp_pattern:
#     reg = re.compile(f'{pattern}')
#     stp_pattern_reg.append(reg)
# stp_pattern_reg = stp_pattern_reg

def integ_tbl(db_access, table_name_list, columns):
    '''db에서 테이블 가져온 후 데이터 프레임 통합 (concat)'''

    df = pd.DataFrame()
    for tbl in table_name_list:
        df_ = db_access.get_tbl(tbl, columns)
        df_.loc[:, 'table_name'] = tbl
        df = pd.concat([df, df_])
        
    # sort
    df = df.sort_values(by='brand_name').reset_index(drop=True)
    
    return df
    
class TitlePreProcess:
    
    def __init__(self):
        # 추출해야 할 단어 정규식 표현
        volume_ml = re.compile('[x]*\s*[0-9]*[.]?[0-9]+\s*[m]*[l]+')
        volume_kg = re.compile('[x]*\s*[0-9]*[.]?[0-9]+\s*[mk]*[g]+')
        volume_oz = re.compile('[x]*\s*[0-9]*[.]?[0-9]+\s*[fl]*\s*[ozounceounce온스]+')
        self.extract_reg = [volume_ml, volume_kg, volume_oz]


        # 유지해야 할 단어 정규식 
        num_0 = re.compile('[a-z]*\s*[0-9]+\s*호')
        num_1 = re.compile('#\s*[a-z]*\s*[0-9]+')
        num_2 = re.compile('[n]+[oO]+[.]?\s*[0-9]+')
        n_in_one = re.compile('[0-9]+\s?in\s?[0-9]+')
        spf = re.compile('spf\s*[0-9]*[+]*')
        pa = re.compile('pa\s*[0-9]*[+]+')
        self.keep_wd_reg = [num_0, num_1, num_2, n_in_one, spf, pa]
        self.keep_wd_list = ['num_0', 'num_1', 'num_2', 'n_in_one', 'spf', 'pa']


        # 불용어 패턴 정규식
        stp_pattern = [
        '[총x]*\s*[0-9]*[.]?[0-9]+\s*[eaEA]+\s*[0-9]*\s*[씩]?\s*[더]?',
        '[총x]*\s*[0-9]*[.]?[0-9]+\s*[매]+[입]?\s*[0-9]*\s*[씩]?\s*[더]?',
        '[총x]*\s*[0-9]*[.]?[0-9]+\s*[개]+[입]?\s*[0-9]*\s*[씩]?\s*더?',
        '[총x]*\s*[0-9]*[.]?[0-9]+\s*[팩]+\s*[0-9]*\s*[씩]?\s*[더]?',
        '[총x]*\s*[0-9]*[.]?[0-9]+\s*[장]+\s*[0-9]*\s*[씩]?\s*[더]?',
        '[총x]*\s*[0-9]*[.]?[0-9]+\s*[p]+\s*[0-9]*\s*[씩]?\s*[더]?',
        ]
        stp_pattern_reg = []
        for pattern in stp_pattern:
            reg = re.compile(f'{pattern}')
            stp_pattern_reg.append(reg)
        self.stp_pattern_reg = stp_pattern_reg
        
    def extract_info(self, title):    
        '''volumne(용량) 추출'''
        for r in self.extract_reg:
            r_ = r.findall(title)

            if len(r_) == 0:
                pass
            
            elif len(r_) == 1:
                title = title.replace(r_[0], ' ')
                ext = r_[0]
                
            else:
                for elm in r_:
                    title = title.replace(elm, ' ')
                ext = str(r_)

        '''유지 할 문자 추출'''
        keep_wd_dict = {}
        i = 0
        for r in self.keep_wd_reg:
            r_ = r.findall(title)

            if len(r_) == 0:
                pass

            elif len(r_) == 1:
                keep_wd = r_[0]
                title = title.replace(keep_wd, ' ')
                
                keep_wd = keep_wd.replace(' ', '')
                keep_wd_dict[self.keep_wd_list[i]] = keep_wd

            else:
                for elm in r_:
                    title = title.replace(elm, ' ')
                    
                keep_wd = max(r_, key=lambda x: len(x)) # 길이가 가장 긴 원소 채택
                keep_wd = keep_wd.replace(' ', '')
                keep_wd_dict[self.keep_wd_list[i]] = keep_wd
                
            i += 1
            
        title = re.sub(' +', ' ', title)
        title = title.strip()
        
        if title == '':
            title = np.nan

        return title, keep_wd_dict

    def remove_stp_pattern(self, title):           
        '''불용어 패턴 제거'''
        
        for pattern in self.stp_pattern_reg:
            title = pattern.sub(' ', title)

            
        '''상품명에서 한글만 추출'''
        title = re.sub('[^가-힣]', ' ', title)
        title = re.sub(' +', ' ', title)
        title = title.strip()
        
        if title == '':
            title = np.nan

        return title


    def remove_dup_words(self, title):
        '''타이틀 중복 토큰 제거'''

        word_sp = title.split(' ')
        org_idx = list(range(len(word_sp)))
        
        # 중복단어 index
        dup_index = [i for i, v in enumerate(word_sp) if v in word_sp[:i]]
        drop_index = list(set(dup_index))
        for i in drop_index:
            org_idx.remove(i)

        title_dedup = ' '.join([word_sp[i] for i in org_idx])
        
        if title == '':
            title = np.nan
            
        return title_dedup


    def insert_keep_wd(self, title, keep_wd_dict):
        '''유지 할 문자 삽입'''
        
        word_sp = title.split(' ')
        keep_wd_set = list(set(keep_wd_dict.values()))

        title_sp = word_sp + keep_wd_set
        
        title_keep_wds = ' '.join(title_sp)
        
        title_keep_wds = re.sub(' +', ' ', title_keep_wds)
        title_keep_wds = title_keep_wds.strip()
        
        if title_keep_wds == '':
            title_keep_wds = np.nan
            
        return title_keep_wds

                    
    def title_preprocessor(self, title, brand):
        ''' preprocessing product name '''
        
        title = str(title.lower())
        brand = str(brand.lower())
        title_ = title.replace('해외', '').replace('단종', '')
        title_ = title_.replace(brand, '')
        title_ = re.sub(' +', ' ', title_).strip()

        '''상품명에서 상품 정보 추출'''
        if str(title_) == '':
            return title
        else:
            return_data = self.extract_info(title_)
            title_0 = return_data[0]
            keep_wd_dict = return_data[1]

        '''불용어 패턴 제거 및 한글 추출'''
        if str(title_0) == 'nan':
            return title_
        else:
            title_1 = self.remove_stp_pattern(title_0)

        '''토큰 중복 제거'''
        if str(title_1) == 'nan':
            return title_0
        else:
            title_2 = self.remove_dup_words(title_1)

        '''유지 할 문자 삽입'''
        if str(title_2) == 'nan':
            return title_1
        elif len(keep_wd_dict) == 0:
            return title_2
        else:
            title_3 = self.insert_keep_wd(title_2, keep_wd_dict)
            return title_3
        
    def categ_reclassifier(self, input_data: pd.DataFrame, source: int) -> pd.DataFrame:
        '''
        카테고리 재분류
        
        Input_Data
        - input_data: 카테고리 정보가 할당된 데이터 프레임 
        
        ** necessary columns: ['id', 'selection', 'division', 'groups']
        
        - source: 'naver' or 'glowpick' 
        
        '''
        
        categ_list_0 = [
            {'립메이크업': '립메이크업',
            '컨투어링': '컨투어링',
            '페이스메이크업': '베이스메이크업',
            '배쓰&바디': '바디케어',
            '선케어': '선케어',
            '네일': '네일케어',
            '클렌징': '클렌징',
            '마스크/팩': '마스크/팩',
            '헤어': '헤어케어',
            '남성화장품': '남성화장품'},
            {'에센스/세럼': '에센스',
            '로션/에멀젼': '로션',
            '스킨/토너': '스킨/토너',
            '크림': '크림',
            '마스카라': '마스카라',
            '아이섀도우': '아이섀도',
            '아이라이너': '아이라이너',
            '아이브로우': '아이브로',
            '미스트': '스킨/토너'},
            {'여성향수': '프래그런스', '유니섹스향수': '프래그런스', '남성향수': '프레그런스'}]
        
        categ_list_1 = [
            {},
            {'선케어': '선케어',
            '클렌징': '클렌징',
            '마스크/팩': '마스크/팩',
            '베이스메이크업': '베이스메이크업',
            '헤어케어': '헤어케어',
            '헤어스타일링': '헤어케어',
            '헤어소품': '헤어케어',
            '네일케어': '네일케어',
            '바디케어': '바디케어',
            '남성화장품': '남성화장품'},
            {'로션': '로션',
            '올인원': '로션',
            '에센스': '에센스',
            '크림': '크림',
            '톤업크림': '크림',
            '아이케어': '크림',
            '스킨/토너': '스킨/토너',
            '미스트': '스킨/토너',
            '스킨': '스킨/토너',
            '여성향수': '프래그런스',
            '남녀공용향수': '프래그런스',
            '남성향수': '프래그런스',
            '립스틱': '립메이크업',
            '립틴트': '립메이크업',
            '립글로스': '립메이크업',
            '립라이너': '립메이크업',
            '립케어': '립메이크업',
            '블러셔': '컨투어링',
            '아이섀도': '아이섀도',
            '아이브로': '아이브로',
            '마스카라': '마스카라',
            '속눈썹영양제': '마스카라',
            '아이라이너': '아이라이너',
            '하이라이터/쉐이딩': '컨투어링'}]
            
        if source == 0:
            categs = categ_list_0
        
        elif source == 1:
            categs = categ_list_1
            
        selection = categs[0]
        division = categs[1]
        groups = categs[2]
        
        
        df = input_data.copy()
        
        for idx in tqdm(range(len(df))):
            category = ''
            
            sel = df.loc[idx, 'selection']
            if sel in selection:
                category = selection[sel]
                
            else:
                div = df.loc[idx, 'division']
                if div in division:
                    category = division[div]
                    
                else:
                    grp = df.loc[idx, 'groups']
                    if grp in groups:
                        category = groups[grp]
                        
            if category == '':
                category = np.nan
            df.loc[idx, 'category'] = category
        
        return df



# def extract_info(title, extract_reg, keep_wd_reg, keep_wd_list):    
#     '''volumne(용량) 추출'''
#     for r in extract_reg:
#         r_ = r.findall(title)

#         if len(r_) == 0:
#             pass
        
#         elif len(r_) == 1:
#             title = title.replace(r_[0], ' ')
#             ext = r_[0]
            
#         else:
#             for elm in r_:
#                 title = title.replace(elm, ' ')
#             ext = str(r_)

#     '''유지 할 문자 추출'''
#     keep_wd_dict = {}
#     i = 0
#     for r in keep_wd_reg:
#         r_ = r.findall(title)

#         if len(r_) == 0:
#             pass

#         elif len(r_) == 1:
#             keep_wd = r_[0]
#             title = title.replace(keep_wd, ' ')
            
#             keep_wd = keep_wd.replace(' ', '')
#             keep_wd_dict[keep_wd_list[i]] = keep_wd

#         else:
#             for elm in r_:
#                 title = title.replace(elm, ' ')
                
#             keep_wd = max(r_, key=lambda x: len(x)) # 길이가 가장 긴 원소 채택
#             keep_wd = keep_wd.replace(' ', '')
#             keep_wd_dict[keep_wd_list[i]] = keep_wd
            
#         i += 1
        
#     title = re.sub(' +', ' ', title)
#     title = title.strip()
    
#     if title == '':
#         title = np.nan

#     return title, keep_wd_dict

# def remove_stp_pattern(title, stp_pattern_reg):           
#     '''불용어 패턴 제거'''
    
#     for pattern in stp_pattern_reg:
#         title = pattern.sub(' ', title)

        
#     '''상품명에서 한글만 추출'''
#     title = re.sub('[^가-힣]', ' ', title)
#     title = re.sub(' +', ' ', title)
#     title = title.strip()
    
#     if title == '':
#         title = np.nan

#     return title


# def remove_dup_words(title):
    
#     '''타이틀 중복 토큰 제거'''

#     word_sp = title.split(' ')
#     org_idx = list(range(len(word_sp)))
    
#     # 중복단어 index
#     dup_index = [i for i, v in enumerate(word_sp) if v in word_sp[:i]]
#     drop_index = list(set(dup_index))
#     for i in drop_index:
#         org_idx.remove(i)

#     title_dedup = ' '.join([word_sp[i] for i in org_idx])
    
#     if title == '':
#         title = np.nan
        
#     return title_dedup


# def insert_keep_wd(title, keep_wd_dict):
#     '''유지 할 문자 삽입'''
    
#     word_sp = title.split(' ')
#     keep_wd_set = list(set(keep_wd_dict.values()))

#     title_sp = word_sp + keep_wd_set
    
#     title_keep_wds = ' '.join(title_sp)
    
#     title_keep_wds = re.sub(' +', ' ', title_keep_wds)
#     title_keep_wds = title_keep_wds.strip()
    
#     if title_keep_wds == '':
#         title_keep_wds = np.nan
        
#     return title_keep_wds

                
# def title_preprocessor(title, brand):
#     ''' preprocessing product name '''
    
#     title = str(title.lower())
#     brand = str(brand.lower())
#     title_ = re.sub('[해외단종]', '', title)
#     title_ = title_.replace(brand, '')
#     title_ = re.sub(' +', ' ', title_).strip()

#     '''상품명에서 상품 정보 추출'''
#     if str(title_) == '':
#         return title
#     else:
#         return_data = extract_info(title_, extract_reg, keep_wd_reg, keep_wd_list)
#         title_0 = return_data[0]
#         keep_wd_dict = return_data[1]

#     '''불용어 패턴 제거 및 한글 추출'''
#     if str(title_0) == 'nan':
#         return title_
#     else:
#         title_1 = remove_stp_pattern(title_0, stp_pattern_reg)

#     '''토큰 중복 제거'''
#     if str(title_1) == 'nan':
#         return title_0
#     else:
#         title_2 = remove_dup_words(title_1)

#     '''유지 할 문자 삽입'''
#     if str(title_2) == 'nan':
#         return title_1
#     elif len(keep_wd_dict) == 0:
#         return title_2
#     else:
#         title_3 = insert_keep_wd(title_2, keep_wd_dict)
#         return title_3
    
    

class ThreadTitlePreprocess(QtCore.QThread, QtCore.QObject):
    ''' Thread preprocessing product name '''
    
    def __init__(self, parent=None):
        super().__init__()
        self.preprocess = TitlePreProcess()
        
        
    progress = QtCore.pyqtSignal(object)
    def run(self):
        df_0 = pd.read_csv(tbl_cache + '/tbl_0.csv')
        df_1 = pd.read_csv(tbl_cache + '/tbl_1.csv')
        
        df_0 = self.preprocess.categ_reclassifier(df_0, 0)
        df_1 = self.preprocess.categ_reclassifier(df_1, 1)
        df_concat = pd.concat([df_0, df_1])
        
        t = tqdm(range(len(df_concat)))
        idx = 0
        for i in t:
            self.progress.emit(t)
            
            if idx >= len(df_0):
                idx_ = idx - len(df_0) 
                title = df_1.loc[idx_, 'product_name']
                brand = df_1.loc[idx_, 'brand_name']
                title_ = self.preprocess.title_preprocessor(title, brand)
                df_1.loc[idx_, 'product_name'] = str(title_)
            
            else:
                title = df_0.loc[idx, 'product_name']
                brand = df_0.loc[idx, 'brand_name']
                title_ = self.preprocess.title_preprocessor(title, brand)
                df_0.loc[idx, 'product_name'] = str(title_)
            
            idx += 1
            
        df_0.to_csv(tbl_cache + '/deprepro_0.csv', index=False)
        df_1.to_csv(tbl_cache + '/deprepro_1.csv', index=False)
        
        
    
        
        # t2 = tqdm(range(len(df_1)))
        # for i in t2:
        #     progress.emit(t2)
        #     title = df_1.loc[i, 'product_name']
        #     brand = df_1.loc[i, 'brand_name']
        #     title_ = TitlePreProcess().title_preprocessor(title, brand)
        #     df_1.loc[i, 'product_name'] = str(title_)
        # df_1.to_csv(tbl_cache + '/deprepro_1.csv', index=False)
        
    # def run(self):
        
    #     ''' 상품명 전처리 함수
        
    #     -- input data -- 
    #     input_datafram: 전처리 대상 상품명 할당된 데이터프레임
    #         <필수 컬럼>
    #         'id' : 상품 아이디 (pk) (int)
    #         'brand_name' : 상품 브랜드명 (str)
    #         'product_name' : 상품명 (str)
            
    #     '''

    #     # 추출해야 할 단어 정규식 표현
    #     volume_ml = re.compile('[x]*\s*[0-9]*[.]?[0-9]+\s*[m]*[l]+')
    #     volume_kg = re.compile('[x]*\s*[0-9]*[.]?[0-9]+\s*[mk]*[g]+')
    #     volume_oz = re.compile('[x]*\s*[0-9]*[.]?[0-9]+\s*[fl]*\s*[ozounceounce온스]+')
    #     extract_reg = [volume_ml, volume_kg, volume_oz]


    #     # 유지해야 할 단어 정규식 
    #     num_0 = re.compile('[a-z]*\s*[0-9]+\s*호')
    #     num_1 = re.compile('#\s*[a-z]*\s*[0-9]+')
    #     num_2 = re.compile('[n]+[oO]+[.]?\s*[0-9]+')
    #     n_in_one = re.compile('[0-9]+\s?in\s?[0-9]+')
    #     spf = re.compile('spf\s*[0-9]*[+]*')
    #     pa = re.compile('pa\s*[0-9]*[+]+')
    #     keep_wd_reg = [num_0, num_1, num_2, n_in_one, spf, pa]
    #     keep_wd_list = ['num_0', 'num_1', 'num_2', 'n_in_one', 'spf', 'pa']


    #     # 불용어 패턴 정규식
    #     stp_pattern = [
    #     '[총x]*\s*[0-9]*[.]?[0-9]+\s*[eaEA]+\s*[0-9]*\s*[씩]?\s*[더]?',
    #     '[총x]*\s*[0-9]*[.]?[0-9]+\s*[매]+[입]?\s*[0-9]*\s*[씩]?\s*[더]?',
    #     '[총x]*\s*[0-9]*[.]?[0-9]+\s*[개]+[입]?\s*[0-9]*\s*[씩]?\s*더?',
    #     '[총x]*\s*[0-9]*[.]?[0-9]+\s*[팩]+\s*[0-9]*\s*[씩]?\s*[더]?',
    #     '[총x]*\s*[0-9]*[.]?[0-9]+\s*[장]+\s*[0-9]*\s*[씩]?\s*[더]?',
    #     '[총x]*\s*[0-9]*[.]?[0-9]+\s*[p]+\s*[0-9]*\s*[씩]?\s*[더]?',
    #     ]

    #     stp_pattern_reg = []
    #     for pattern in stp_pattern:
    #         reg = re.compile(f'{pattern}')
    #         stp_pattern_reg.append(reg)
            
    #     tbl_0 = pd.read_csv(tbl_cache + '/tbl_0.csv')

    #     # 전처리 완료 된 데이터 할당 df
    #     df_deprepro = tbl_0.copy()
        
    #     # lower
    #     titles = df_deprepro.product_name.str.lower().tolist()

    #     idx = 0 
    #     t = tqdm(titles)
    #     for title in t:
    #         self.progress.emit(t)
            
    #         # title = df_deprepro.loc[idx, 'product_name'].lower()
    #         brand = df_deprepro.loc[idx, 'brand_name'].lower()
    #         brand = str(brand)
    #         title = str(title)
    #         title_ = re.sub('[해외단종]', '', title)
    #         title_ = title_.replace(brand, '')
    #         title_ = re.sub(r' +', ' ', title_).strip()

    #         '''상품명에서 상품 정보 추출'''
    #         if str(title_) == '':
    #             df_deprepro.loc[idx, 'product_name_'] = str(title)
    #             idx += 1
    #             continue
    #         else:
    #             return_data = extract_info(title_, extract_reg, keep_wd_reg, keep_wd_list)
    #             title_0 = return_data[0]
    #             keep_wd_dict = return_data[1]

    #         '''불용어 패턴 제거 및 한글 추출'''
    #         if str(title_0) == 'nan':
    #             df_deprepro.loc[idx, 'product_name_'] = str(title_)
    #             idx += 1
    #             continue
    #         else:
    #             title_1 = remove_stp_pattern(title_0, stp_pattern_reg)

    #         '''토큰 중복 제거'''
    #         if str(title_1) == 'nan':
    #             df_deprepro.loc[idx, 'product_name_'] = str(title_0)
    #             idx += 1
    #             continue
    #         else:
    #             title_2 = remove_dup_words(title_1)

    #         '''유지 할 문자 삽입'''
    #         if str(title_2) == 'nan':
    #             df_deprepro.loc[idx, 'product_name_'] = str(title_1)
    #             idx += 1
    #             continue
    #         elif len(keep_wd_dict) == 0:
    #             df_deprepro.loc[idx, 'product_name_'] = str(title_2)
    #             idx += 1
    #             continue
    #         else:
    #             title_3 = insert_keep_wd(title_2, keep_wd_dict)
    #             df_deprepro.loc[idx, 'product_name_'] = str(title_3)

    #         idx += 1
            
    #     df_deprepro = df_deprepro.rename(columns={'product_name': 'product_name_old'}).rename(columns={'product_name_': 'product_name'})
        
    #     df_deprepro.to_csv(tbl_cache + '/deprepro_0.csv', index=False)
      

        
        
def categ_reclassifier(input_data: pd.DataFrame, source: int) -> pd.DataFrame:
    '''
    카테고리 재분류
    
    Input_Data
    - input_data: 카테고리 정보가 할당된 데이터 프레임 
    
    ** necessary columns: ['id', 'selection', 'division', 'groups']
    
    - source: 'naver' or 'glowpick' 
    
    '''
    
    categ_list_0 = [
        {'립메이크업': '립메이크업',
        '컨투어링': '컨투어링',
        '페이스메이크업': '베이스메이크업',
        '배쓰&바디': '바디케어',
        '선케어': '선케어',
        '네일': '네일케어',
        '클렌징': '클렌징',
        '마스크/팩': '마스크/팩',
        '헤어': '헤어케어',
        '남성화장품': '남성화장품'},
        {'에센스/세럼': '에센스',
        '로션/에멀젼': '로션',
        '스킨/토너': '스킨/토너',
        '크림': '크림',
        '마스카라': '마스카라',
        '아이섀도우': '아이섀도',
        '아이라이너': '아이라이너',
        '아이브로우': '아이브로',
        '미스트': '스킨/토너'},
        {'여성향수': '프래그런스', '유니섹스향수': '프래그런스', '남성향수': '프레그런스'}]
    
    categ_list_1 = [
        {},
        {'선케어': '선케어',
        '클렌징': '클렌징',
        '마스크/팩': '마스크/팩',
        '베이스메이크업': '베이스메이크업',
        '헤어케어': '헤어케어',
        '헤어스타일링': '헤어케어',
        '헤어소품': '헤어케어',
        '네일케어': '네일케어',
        '바디케어': '바디케어',
        '남성화장품': '남성화장품'},
        {'로션': '로션',
        '올인원': '로션',
        '에센스': '에센스',
        '크림': '크림',
        '톤업크림': '크림',
        '아이케어': '크림',
        '스킨/토너': '스킨/토너',
        '미스트': '스킨/토너',
        '스킨': '스킨/토너',
        '여성향수': '프래그런스',
        '남녀공용향수': '프래그런스',
        '남성향수': '프래그런스',
        '립스틱': '립메이크업',
        '립틴트': '립메이크업',
        '립글로스': '립메이크업',
        '립라이너': '립메이크업',
        '립케어': '립메이크업',
        '블러셔': '컨투어링',
        '아이섀도': '아이섀도',
        '아이브로': '아이브로',
        '마스카라': '마스카라',
        '속눈썹영양제': '마스카라',
        '아이라이너': '아이라이너',
        '하이라이터/쉐이딩': '컨투어링'}]
        
    if source == 0:
        categs = categ_list_0
    
    elif source == 1:
        categs = categ_list_1
        
    selection = categs[0]
    division = categs[1]
    groups = categs[2]
    
    
    df = input_data.copy()
    
    for idx in tqdm(range(len(df))):
        category = ''
        
        sel = df.loc[idx, 'selection']
        if sel in selection:
            category = selection[sel]
            
        else:
            div = df.loc[idx, 'division']
            if div in division:
                category = division[div]
                
            else:
                grp = df.loc[idx, 'groups']
                if grp in groups:
                    category = groups[grp]
                    
        if category == '':
            category = np.nan
        
        df.loc[idx, 'category'] = category
        
        
    return df