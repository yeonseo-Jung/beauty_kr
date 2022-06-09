import os
import re
import sys
from tqdm.auto import tqdm

import numpy as np
import pandas as pd

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
    tbl_cache = os.path.join(root, 'tbl_cache')

from PyQt5 import uic
from PyQt5 import QtCore
from PyQt5.QtWidgets import *

class TitlePreProcess:
    def __init__(self):
        
        # 제거해야 할 단어 정규식 표현
        spf = re.compile('spf\s*[0-9]*[+]*')
        pa = re.compile('pa\s*[0-9]*[+]+')        
        self.extract_reg = [spf, pa]

        # 유지해야 할 단어 정규식 
        volume_ml = re.compile('[x]*\s*[0-9]*[.]?[0-9]+\s*[m]*l')
        volume_kg = re.compile('[x]*\s*[0-9]*[.]?[0-9]+\s*[mk]*g')
        volume_oz = re.compile('[x]*\s*[0-9]*[.]?[0-9]+\s*[fl]*\s*oz')
        num_0 = re.compile('[a-z]*\s*[0-9]+\s*호')
        num_1 = re.compile('#\s*[a-z]*\s*[0-9]+')
        num_2 = re.compile('[n]+[o]+[.]?\s*[0-9]+')
        # n_in_one = re.compile('[0-9]+\s?in\s?[0-9]+')
        self.keep_wd_reg = [volume_ml, volume_kg, volume_oz, num_0, num_1, num_2]
        self.keep_wd_list = ['volume_ml', 'volume_kg', 'volume_oz', 'num_0', 'num_1', 'num_2']

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
        
        '''제거 할 문자 추출'''
        for r in self.extract_reg:
            r_ = r.findall(title)

            if len(r_) == 0:
                pass
            
            elif len(r_) == 1:
                title = title.replace(r_[0], ' ')
                
            else:
                for elm in r_:
                    title = title.replace(elm, ' ')

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

        # '''상품명에서 한글, 영문, 숫자, .만 추출'''
        title = re.sub('[^가-힣a-z0-9.]', ' ', title)
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
        
        title = str(title).lower()
        brand = str(brand).lower()
        title_ = title.replace('해외', ' ').replace('단종', ' ').replace(brand, ' ')
        title_ = re.sub(' +', ' ', title_).strip()

        '''상품명에서 상품 정보 추출'''
        if str(title_) == '':
            return title, {}
        else:
            return_data = self.extract_info(title_)
            title_0 = return_data[0]
            keep_wd_dict = return_data[1]

        '''불용어 패턴 제거 및 한글 추출'''
        if str(title_0) == 'nan':
            return title_, keep_wd_dict
        else:
            title_1 = self.remove_stp_pattern(title_0)

        '''토큰 중복 제거'''
        if str(title_1) == 'nan':
            return title_0, keep_wd_dict
        else:
            title_2 = self.remove_dup_words(title_1)
        
        if str(title_2) == 'nan':
            return title_1, keep_wd_dict
        else:
            return title_2, keep_wd_dict
        
    def categ_reclassifier(self, input_data: pd.DataFrame, source: int) -> pd.DataFrame:
        '''
        카테고리 재분류
        
        Input_Data
        - input_data: 카테고리 정보가 할당된 데이터 프레임 
        
        ** necessary columns: ['id', 'selection', 'division', 'groups']
        
        - source: glowpick(0) or naver(1)
        
        '''
        
        # Category Sync
        categ_list_0 = [
            {
                '스킨케어': '스킨케어',
                '립메이크업': '메이크업',
                '페이스메이크업': '메이크업',
                '아이메이크업': '메이크업',
                '컨투어링': '메이크업',
                '배쓰&바디': '바디케어',
                '헤어': '헤어케어',
                '클렌징': '클렌징',
                '남성화장품': '맨즈케어',
                '프래그런스': '프래그런스',
                '선케어': '선케어',
                '마스크/팩': '마스크/팩',
                '뷰티툴': '뷰티툴',
                '베이비': '베이비'
             },
            {
                }, 
            {
                }
        ]
        categ_list_1 = [
            {
                '출산/육아': '베이비'
                },
            {
                '스킨케어': '스킨케어',
                '베이스메이크업': '메이크업',
                '색조메이크업': '메이크업',
                '바디케어': '바디케어',
                '헤어케어': '헤어케어',
                '헤어스타일링': '헤어케어',
                '클렌징': '클렌징',
                '남성화장품': '맨즈케어',
                '향수': '프래그런스',
                '선케어': '선케어',
                '마스크/팩': '마스크/팩',
                '뷰티소품': '뷰티툴',
            },
            {
                
            }
        ]
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

class ThreadTitlePreprocess(QtCore.QThread, QtCore.QObject):
    ''' Thread preprocessing product name '''
    
    def __init__(self, parent=None):
        super().__init__()
        self.preprocess = TitlePreProcess()
        self.power = True
        
        
    progress = QtCore.pyqtSignal(object)
    def run(self):
            
        df_0 = pd.read_csv(tbl_cache + '/tbl_0.csv')
        # 글로우픽 내부 중복 제거
        df_0 = df_0[df_0.dup_check != -1].reset_index(drop=True)
        df_1 = pd.read_csv(tbl_cache + '/tbl_1.csv')
        
        df_0 = self.preprocess.categ_reclassifier(df_0, 0)
        df_1 = self.preprocess.categ_reclassifier(df_1, 1)
        
        t = tqdm(range(len(df_0) + len(df_1)))
        idx = 0
        for i in t:
            if self.power == True:
                self.progress.emit(t)
                
                if idx >= len(df_0):
                    idx_ = idx - len(df_0) 
                    title = df_1.loc[idx_, 'product_name']
                    brand = df_1.loc[idx_, 'brand_name']
                    title_, keep_wd_dict = self.preprocess.title_preprocessor(title, brand)
                    df_1.loc[idx_, 'product_name'] = str(title_)
                    if len(keep_wd_dict) == 0:
                        df_1.loc[idx_, 'keep_words'] = np.nan
                    else:
                        df_1.loc[idx_, 'keep_words'] = str(keep_wd_dict)
                
                else:
                    title = df_0.loc[idx, 'product_name']
                    brand = df_0.loc[idx, 'brand_name']
                    title_, keep_wd_dict = self.preprocess.title_preprocessor(title, brand)
                    df_0.loc[idx, 'product_name'] = str(title_)
                    if len(keep_wd_dict) == 0:
                        df_0.loc[idx, 'keep_words'] = np.nan
                    else:
                        df_0.loc[idx, 'keep_words'] = str(keep_wd_dict)
                
                idx += 1
            
            else:
                self.progress.emit(t)
                break
            
        if idx == len(df_0) + len(df_1):
            df_0.to_csv(tbl_cache + '/deprepro_0.csv', index=False)
            df_1.to_csv(tbl_cache + '/deprepro_1.csv', index=False)
        
    def stop(self):
        ''' Stop Thread '''
        
        self.power = False
        self.quit()
        self.wait(3000)