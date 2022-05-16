# necessary
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

# db connection 
import pymysql
from sqlalchemy import create_engine


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


    def get_tbl(self, table_name, columns):
        
        ''' db에서 원하는 테이블, 컬럼 pd.DataFrame에 할당 '''
        
        curs = AccessDataBase(self.user_name, self.password, self.db_name).db_connect()
        
        if columns == 'all':
            sql_query = f'SELECT * FROM {table_name};'
            
        
        else:
            # SELECT columns
            sql_query = 'SELECT '
            i = 0
            for col in columns:
                if i == 0:
                    sql_query += col

                else:
                    sql_query += ', ' + col

                i += 1

            # FROM table_name
            sql_query += f' FROM {table_name};'
        
        curs.execute(sql_query)
        tbl = curs.fetchall()
        df = pd.DataFrame(tbl)
        curs.close()
        
        return df

    def engine_upload(self, upload_df, table_name):
        host_url = "db.ds.mycelebs.com"
        port_num = 3306
        engine = create_engine(f'mysql+pymysql://{self.user_name}:{self.password}@{host_url}:{port_num}/{self.db_name}?charset=utf8mb4')
        engine_conn = engine.connect()
        upload_df.to_sql(table_name, engine_conn, if_exists='append', index=None)
        engine_conn.close()
        engine.dispose()


