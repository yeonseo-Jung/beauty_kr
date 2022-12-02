import os
import re
import sys
import ast
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
        
    def categ_reclassifier(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
        카테고리 재분류
        
        Input_Data
        - df: 카테고리 정보가 할당된 데이터 프레임 
            ** necessary columns: ['id', 'selection', 'division', 'groups', 'table_name']  **
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
        categ_list_2 = [
            {
                '스킨케어': '스킨케어',
                '메이크업': '메이크업',
                '선케어': '선케어',
                '클렌징': '클렌징',
                '마스크팩': '마스크/팩',
                '헤어케어': '헤어케어',
                '남성': '맨즈케어',
                '향수/디퓨저': '향수/디퓨저',
                '베이비': '베이비',
                '미용소품': '뷰티툴',
            },
            {
                '마스크팩': '마스크/팩',
                '미스트/오일': '스킨케어',
                '바디케어': '바디케어',
                '선케어': '선케어',
                '스킨/로션': '스킨케어',
                '에센스/크림': '스킨케어',
                '클렌징': '클렌징',
            },
            {
            
            }
        ]        
        
        # select source (table_name)
        df_0 = df.loc[df.table_name=='glowpick_product_info_final_version'].reset_index(drop=True)
        df_2 = df.loc[df.table_name=='oliveyoung_product_info_final_version'].reset_index(drop=True)
        df_1 = df.loc[(df.table_name!='glowpick_product_info_final_version') & (df.table_name!='oliveyoung_product_info_final_version')].reset_index(drop=True)
        
        df_list = []
        if len(df_0) != 0:
            categs = categ_list_0
            df_0 = self._categ_reclassifier(df_0, categs)
            df_list.append(df_0)
            
        if len(df_1) != 0:
            categs = categ_list_1
            df_1 = self._categ_reclassifier(df_1, categs)
            df_list.append(df_1)
        
        if len(df_2) != 0:
            categs = categ_list_2
            df_2 = self._categ_reclassifier(df_2, categs)
            df_list.append(df_2)
                
        df_categs = pd.concat(df_list).reset_index(drop=True)
        return df_categs
    
    def _categ_reclassifier(self, df, categs):
        for idx in tqdm(range(len(df))):
            selection = categs[0]
            division = categs[1]
            groups = categs[2]
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
        df = df[df.category.notnull()].reset_index(drop=True)
        return df

def check_duplicated(grp_df):
    ''' 글로우픽 중복체크 '''
    
    grp_df.loc[(grp_df.status_grp == 1) | (grp_df.status_grp == 3), 'dup_check'] = 0
    grp_df.loc[(grp_df.status_grp == 1) | (grp_df.status_grp == 3), 'dup_id'] = np.nan


    df_l = []

    for i in [2,4,5]:
        grp = grp_df[grp_df.status_grp == i]
        detail_grp = grp.groupby(['brand_code','prd_prepro'])

        if i == 2 or i == 4:
            
            #product_code가 높은것 -> 최우선 매핑 대상
            for key, item in detail_grp:
                df=detail_grp.get_group(key)

                select_prd=max(df.product_code)
                dup_prd_code = list(set(df.product_code) - {max(df.product_code)})
                dup_ids = list(df[df.product_code.isin(dup_prd_code)].id)

                df.loc[df.product_code == select_prd, 'dup_check'] = 1
                df.loc[df.product_code == select_prd, 'dup_id'] = str(dup_ids)
                df.loc[df.id.isin(dup_ids), 'dup_check'] = -1
                df.loc[df.id.isin(dup_ids), 'dup_id'] = np.nan

                df_l.append(df)

        else:
            for key, item in detail_grp:
                df=detail_grp.get_group(key)
                select_prd=df[df.status == 1]

                # 판매중인 상품이 여러개인 경우, product_code가 높은 것 선택
                if len(select_prd) >= 2:
                    select_prd_lst = df.loc[(df.status == 1), 'product_code']
                    select_prd = max(select_prd_lst)
                else:
                    select_prd=max(df.product_code)

                dup_prd_code = list(set(df.product_code) - {select_prd})
                dup_ids = list(df[df.product_code.isin(dup_prd_code)].id)

                df.loc[df.product_code == select_prd, 'dup_check'] = 1
                df.loc[df.product_code == select_prd, 'dup_id'] = str(dup_ids)
                df.loc[df.id.isin(dup_ids), 'dup_check'] = -1
                df.loc[df.id.isin(dup_ids), 'dup_id'] = np.nan
                df_l.append(df)
                
    final_df = pd.concat([grp_df[grp_df.status_grp == 1], grp_df[grp_df.status_grp == 3], pd.concat(df_l)]).sort_values('id', ignore_index=True)
    
    return final_df

# 글로우픽 타이틀 [단종] 제거
def check_status_and_prepro(glowpick_info_df):
    
    df = glowpick_info_df.copy()
    df['status'] = 1
    df.loc[df.product_name.str.contains('단종'), 'status'] = 0
    
    for i in df.index:
        
        prd_name=df.loc[i, 'product_name']
        df.loc[i, 'prd_prepro'] = prd_name
        
        if df.loc[i, 'status'] == 0:
            prd_name_=re.sub(r'\[단종\]', '', prd_name).strip()
            df.loc[i, 'prd_prepro'] = prd_name_
            
    return df

def str_to_lst(values):
    
    lst_val=ast.literal_eval(values)

    if len(lst_val) == 0:
        return np.nan

    else:
        return lst_val
        
# grouping
def grouping(df): 
    
    """ glowpick product duplicate check
    Input data
    df: glowpick_product_info_final_version 
        columns = ['id', 'product_name', 'product_code', 'brand_code']
    
    return values
        - 1: 단종 상품 없이 하나의 상품만 존재
        - 2: 같은 상품을 두개의 url로 할당한 경우
        - 3: 단종 상품만 존재
        - 4: 단종이 여러번 된 상품(판매중 상품은 없고 단종만 있는 경우)
        - 5: 단종 상품 + 판매중 상품
    """
    
    prepro_df = check_status_and_prepro(df)
    
    grouped_df = prepro_df.groupby('brand_code')
    grouped_df_lst = []
    for key, item in tqdm(grouped_df):
        grp_df=grouped_df.get_group(key)

        for j in grp_df.index:
            prd_name = grp_df.loc[j, 'prd_prepro']
            st = grp_df.loc[j, 'status']

            dup_df = grp_df[grp_df.prd_prepro == prd_name]
            dup_status_lst = list(dup_df.status)
            
            # 판매중인 상품만 존재
            if len(dup_df) == 1 and st == 1:
                grp_df.loc[j, 'status_grp'] = int(1)
                
            # 같은 상품을 두개의 url로 할당한 경우 
            elif len(dup_df) >= 2 and (0 not in set(dup_status_lst)):
                grp_df.loc[j, 'status_grp'] = int(2)
            
            # 유일한 단종 상품만 존재
            elif len(dup_df) == 1 and st == 0:
                grp_df.loc[j, 'status_grp'] = int(3)
            
            # 같은 상품이 단종이 여러번 된 경우 (단종만 존재)
            elif len(dup_df) >=2 and (1 not in set(dup_status_lst)):
                grp_df.loc[j, 'status_grp'] = int(4)
            
            # 판매중과 단종상품이 섞여있는 경우
            elif len(dup_df) >= 2 and (1 in dup_status_lst and 0 in dup_status_lst):
                grp_df.loc[j, 'status_grp'] = int(5)
                
        grouped_df_lst.append(grp_df)
    
    grouped_df_final=pd.concat(grouped_df_lst).sort_values('id').reset_index(drop=True)

    final_df = check_duplicated(grouped_df_final)
    final_dfs = final_df.loc[:, ['id', 'status', 'dup_check', 'dup_id']]
    
    return final_dfs