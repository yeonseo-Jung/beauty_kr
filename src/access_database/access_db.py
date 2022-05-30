# necessary
import numpy as np
import pandas as pd
from tqdm.auto import tqdm
import time

# db connection 
import pymysql
import sqlalchemy

class AccessDataBase():
    
    def __init__(self, user_name, password, db_name):
        self.user_name = user_name
        self.password = password
        self.db_name = db_name
    
    def db_connect(self):
        ''' db connect '''

        host_url = "db.ds.mycelebs.com"
        port_num = 3306
        conn = pymysql.connect(host=host_url, user=self.user_name, passwd=self.password, port=port_num, db=self.db_name, charset='utf8')
        curs = conn.cursor(pymysql.cursors.DictCursor)
        return curs

    def get_tbl_name(self):
        ''' db에 존재하는 모든 테이블 이름 가져오기 '''

        curs = self.db_connect()

        # get table name list
        query = "SHOW TABLES;"
        curs.execute(query)
        tables = curs.fetchall()

        table_list = []
        for table in tables:
            tbl = list(table.values())[0]
            table_list.append(tbl)
        curs.close()
        
        return table_list

    def get_tbl_columns(self, table_name):
        ''' 선택한 테이블 컬럼 가져오기 '''

        curs = self.db_connect()

        # get table columns 
        query = f"SHOW FULL COLUMNS FROM {table_name};"
        curs.execute(query)
        columns = curs.fetchall()

        column_list = []
        for column in columns:
            field = column['Field']
            column_list.append(field)
        curs.close()
        
        return column_list

    def get_tbl(self, table_name, columns):
        ''' db에서 원하는 테이블, 컬럼 pd.DataFrame에 할당 '''
        
        st = time.time()
        curs = self.db_connect()
        
        if columns == 'all':
            query = f'SELECT * FROM {table_name};'
        else:
            # SELECT columns
            query = 'SELECT '
            i = 0
            for col in columns:
                if i == 0:
                    query += f"`{col}`"
                else:
                    query += ', ' + f"`{col}`"
                i += 1

            # FROM table_name
            query += f' FROM {table_name};'
        curs.execute(query)
        tbl = curs.fetchall()
        df = pd.DataFrame(tbl)
        curs.close()
        
        ed = time.time()
        print(f'`{table_name}` Import Time: {round(ed-st, 1)}sec\n\n')
        
        return df
    
    def integ_tbl(self, table_name_list, columns):
        ''' 
        db에서 컬럼이 같은 여러개 테이블 가져오기
        db에서 테이블 가져온 후 데이터 프레임 통합 (concat)
        '''

        df = pd.DataFrame()
        for tbl in table_name_list:
            df_ = self.get_tbl(tbl, columns)
            df_.loc[:, 'table_name'] = tbl
            df = pd.concat([df, df_])
        df = df.reset_index(drop=True)
        return df

    def sqlcol(self, dfparam):    
        ''' Convert DataFrame data type to sql data type '''
        
        dtypedict = {}
        for i,j in zip(dfparam.columns, dfparam.dtypes):
            
            if "object" in str(j):
                dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})
                                    
            if "datetime" in str(j):
                dtypedict.update({i: sqlalchemy.types.DateTime()})

            if "float" in str(j):
                dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})

            if "int" in str(j):
                dtypedict.update({i: sqlalchemy.types.INT()})

        return dtypedict

    def engine_upload(self, upload_df, table_name, if_exists_option, pk=None):
        ''' Create Table '''
        
        host_url = "db.ds.mycelebs.com"
        port_num = 3306
        engine = sqlalchemy.create_engine(f'mysql+pymysql://{self.user_name}:{self.password}@{host_url}:{port_num}/{self.db_name}?charset=utf8mb4')
        
        # Create table or Replace table 
        upload_df.to_sql(table_name, engine, if_exists=if_exists_option, index=False)
        
        # Setting pk 
        if pk != None:
            engine.execute(f'ALTER TABLE {table_name} ADD PRIMARY KEY (`{pk}`);')
        else:
            pass
        engine.dispose()

    def table_update(self, table_name, pk, df):
        ''' Table Update from DB
        
        table_name: table name from db
        pk: primary key
        df: dataframe to update 
        
        '''
        try:
            # get table from db
            _df = self.get_tbl(table_name, 'all')
                    
            # 기존에 존재하는 status값 update
            df_update = _df.loc[:, [pk]].merge(df, on=pk, how='inner')

            # 새로운 status값 append
            df_dedup = pd.concat([_df, df]).drop_duplicates(subset=pk, keep=False)
            df_append = pd.concat([df_update, df_dedup]).sort_values(by=pk).reset_index(drop=True)
            
            self.engine_upload(df_append, table_name, "replace", pk=pk)
            
        except Exception as e:
            # 신규 테이블 업로드
            print(e)
            df = df.sort_values(by=pk).reset_index(drop=True)
            self.engine_upload(df, table_name, "replace", pk=pk)