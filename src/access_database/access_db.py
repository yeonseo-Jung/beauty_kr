# necessary
import os
import sys
import time
import pandas as pd
from datetime import datetime

# db connection 
import pymysql
import sqlalchemy

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src = os.path.abspath(os.path.join(cur_dir, os.pardir))
    sys.path.append(src)

from access_database.constants import CreateQuery

class AccessDataBase: 
    
    def __init__(self, user_name, password, db_name):
        # user info & db
        self.user_name = user_name
        self.password = password
        self.db_name = db_name
    
        # today 
        self.today = datetime.today().strftime('%y%m%d')
        
    def db_connect(self):
        ''' db connect '''

        host_url = "db.ds.mycelebs.com"
        port_num = 3306
        conn = pymysql.connect(host=host_url, user=self.user_name, passwd=self.password, port=port_num, db=self.db_name, charset='utf8')
        curs = conn.cursor(pymysql.cursors.DictCursor)
        return conn, curs
    
    def _execute(self, query):
        conn, curs = self.db_connect()
        curs.execute(query)
        data = curs.fetchall()
        conn.commit()
        curs.close()
        conn.close()
        
        return data

    def get_tbl_name(self):
        ''' db에 존재하는 모든 테이블 이름 가져오기 '''

        conn, curs = self.db_connect()

        # get table name list
        query = "SHOW TABLES;"
        curs.execute(query)
        tables = curs.fetchall()

        table_list = []
        for table in tables:
            tbl = list(table.values())[0]
            table_list.append(tbl)
        
        curs.close()
        conn.close()
        
        return table_list

    def get_tbl_columns(self, table_name):
        ''' 선택한 테이블 컬럼 가져오기 '''
        
        conn, curs = self.db_connect()

        # get table columns 
        query = f"SHOW FULL COLUMNS FROM {table_name};"
        curs.execute(query)
        columns = curs.fetchall()

        column_list = []
        for column in columns:
            field = column['Field']
            column_list.append(field)
        
        curs.close()
        conn.close()
        
        return column_list
    
    def insert(self, table: str, fields: tuple, values: tuple) -> None:
        _fields = ''
        for field in fields:
            if _fields == '':
                _fields +=  field
            else:
                _fields += ', ' + field
        _fields_ = '(' + _fields + ')'

        conn, curs = self.db_connect()

        query = f"INSERT INTO `{table}`{_fields_} VALUES{str(values)};"
        curs.execute(query)

        conn.commit()
        curs.close()
        conn.close()


    def get_tbl(self, table_name, columns='all'):
        ''' db에서 원하는 테이블, 컬럼 pd.DataFrame에 할당 '''
        
        if table_name in self.get_tbl_name():
            st = time.time()
            conn, curs = self.db_connect()
            
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
            conn.close()
            
            ed = time.time()
            print(f'`{table_name}` Import Time: {round(ed-st, 1)}sec\n\n')
        else:
            df = None
            print(f'\n\n`{table_name}` does not exist in db')
        
        return df
    
    def integ_tbl(self, table_name_list, columns='all'):
        ''' 
        db에서 컬럼이 같은 여러개 테이블 가져오기
        db에서 테이블 가져온 후 데이터 프레임 통합 (concat)
        '''
        
        df_list = []
        for tbl in table_name_list:
            df_ = self.get_tbl(tbl, columns)
            df_.loc[:, 'table_name'] = tbl
            df_list.append(df_)
        df = pd.concat(df_list).reset_index(drop=True)
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
        ''' Upload Table into DB '''
        
        host_url = "db.ds.mycelebs.com"
        port_num = 3306
        
        # engine
        engine = sqlalchemy.create_engine(f'mysql+pymysql://{self.user_name}:{self.password}@{host_url}:{port_num}/{self.db_name}?charset=utf8mb4')
        
        # Create table or Replace table 
        upload_df.to_sql(table_name, engine, if_exists=if_exists_option, index=False)
        
        # Setting pk 
        if pk != None:
            engine.execute(f'ALTER TABLE {table_name} ADD PRIMARY KEY (`{pk}`);')
        else:
            pass
        
        engine.dispose()
        print(f'\nTable Upload Success: `{table_name}`')
                
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
    
    def _backup(self, table_name, keep=False):
            
            conn, curs = self.db_connect()
            
            table_list = self.get_tbl_name()
            if table_name in table_list:
                backup_table_name = f'{table_name}_bak_{self.today}'
                
                # 백업 테이블이 이미 존재하는경우 rename
                i = 1
                while backup_table_name in table_list:
                    backup_table_name = backup_table_name + f'_{i}'
                    i += 1

                if keep:
                    query = f'CREATE TABLE {backup_table_name} SELECT * FROM {table_name};'
                else:
                    query = f'ALTER TABLE {table_name} RENAME {backup_table_name};'
                curs.execute(query)
                print(f'\n\n`{table_name}` is backuped successful!\nbackup_table_name: {backup_table_name}')
            else:
                print(f'\n\n`{table_name}` does not exist in db')
            
            conn.commit()
            curs.close()
            conn.close()
        
    def create_table(self, upload_df, table_name, append=False):
        ''' Create table '''
        
        cq = CreateQuery(table_name)
        if table_name in cq.query_dict.keys():
            query = cq.query_dict[table_name]
        else:
            query = None
        
        if query == None:
            print('Table creating query is None')
        else:
            table_list = self.get_tbl_name()
            conn, curs = self.db_connect()
            if not append:
                # backup table
                self._backup(table_name)
            
                # create table
                curs.execute(query)
            else:
                if table_name in table_list:
                    pass
                else:
                    # create table
                    curs.execute(query)
            
            # upload table
            self.engine_upload(upload_df, table_name, if_exists_option='append')
            
            # drop temporary table
            if  f'{table_name}_temp' in table_list:
                curs.execute(f'DROP TABLE {table_name}_temp;')
            
            # commit & close
            conn.commit()
            curs.close()
            conn.close()