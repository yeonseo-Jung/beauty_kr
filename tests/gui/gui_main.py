import os
import re
import sys
import time
import pickle
import pandas as pd
from tqdm.auto import tqdm

cur_dir = os.path.dirname(os.path.realpath(__file__))
root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
src = os.path.abspath(os.path.join(cur_dir, os.pardir))
tbl_cache = root + '/tbl_cache'
sys.path.append(root)
sys.path.append(src)
sys.path.append(tbl_cache)

from access_database import access_db
from mapping import preprocessing
from mapping.preprocessing import ThreadTitlePreprocess
from mapping import mapping_product

from gui.get_table import GetDialog
from gui.scraping import CrawlingWindow


import sys
from PyQt5 import uic
from PyQt5 import QtCore
from PyQt5.QtWidgets import *

mapping_form = uic.loadUiType(cur_dir + '/form/mappingWindow.ui')[0]

class MappingWindow(QMainWindow, mapping_form):
    ''' Product Mapping Window '''
    
    def __init__(self):
        super().__init__()    
        self.setupUi(self)
        self.setWindowTitle('Mapping Product')
        
        self.textBrowser.setOpenExternalLinks(True)
        
        # db 연결
        with open(cur_dir + '/conn.txt', 'rb') as f:
            conn = pickle.load(f)
        self.db = access_db.AccessDataBase(conn[0], conn[1], conn[2])
        
        
        
        self.view_table_name.clicked.connect(self.connect_dialog)
        self.get_table.clicked.connect(self.get_tbl)
        self.preprocess.clicked.connect(self.preprocess_)

        self.compare.clicked.connect(self.mapping_prd)
        self.mapping.clicked.connect(self.select_mapped_prd)
        

        
    def update_progress(self, progress):
        
        prg_dict = progress.format_dict
        itm = prg_dict['n'] 
        tot = prg_dict['total']
        per = round((itm / tot) * 100, 0)
        elapsed = round(prg_dict['elapsed'], 0)
        if itm >= 1:
            remain_time = round((elapsed * tot / itm) - elapsed, 0)
        else:
            remain_time = 0
        
        self.pbar_2.setValue(per)
        
        message = f"{int(per)}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed}s < Remain time: {remain_time}s "
        self.statusbar.showMessage(message)
        
    def connect_dialog(self):
        ''' get.GetDialog connect '''    
        
        self.get = GetDialog()
        self.get.tables.connect(self.append_text)
        self.get.show()
        
    def append_text(self, tables):
        self.textBrowser.clear()
        self.textBrowser.append(tables)
        
        
    def get_tbl(self):
        ''' 데이터 베이스에서 테이블 가져와서 통합하기 '''
        
        # 상품 매핑에 필요한 컬럼
        columns = ['id', 'brand_name', 'product_name', 'selection', 'division', 'groups']
        
        # 매핑 기준 테이블 
        tbl_0 = self.db.get_tbl('glowpick_product_info_final_version', columns)
        tbl_0.loc[:, 'table_name'] = 'glowpick_product_info_final_version'
        tbl_0.to_csv(tbl_cache + '/tbl_0.csv', index=False)
        
        # 매핑 대상 테이블
        tbl_ = self.textBrowser.toPlainText()
        tbls = tbl_.split('\n')
        
        print(f'\n\n\n{tbls}\n\n\n')
        tbl_1 = preprocessing.integ_tbl(self.db, tbls, columns)
        tbl_1.to_csv(tbl_cache + '/tbl_1.csv', index=False)
        
        msg = QMessageBox()
        msg.setText(f'Table import success!')
        msg.exec_()
        
        
    def preprocess_(self):
        ''' 쓰레드 연결 및 함수 수행 ''' 
        
        self.thread = ThreadTitlePreprocess()
        self.thread.progress.connect(self.update_progress)
        self.thread.start()
        
        
        
    # def get_tbl(self):
    #     ''' 데이터 베이스에서 테이블 가져와서 통합하기 '''
        
    #     tables_0 = self.text_0.toPlainText().replace(' ', '').split(',')
    #     tables_1 = self.text_1.toPlainText().replace(' ', '').split(',')
        
    #     # 테이블 통합 후 캐시폴더에 저장 
        
    #     # 상품 매핑에 필요한 컬럼
    #     columns = ['id', 'brand_name', 'product_name', 'selection', 'division', 'groups']
        
    #     # 매핑 기준 테이블
    #     tbl_0 = preprocessing.integ_tbl(self.db, tables_0, columns)
    #     tbl_0.to_csv(tbl_cache + '/tbl_0.csv', index=False)
        
    #     # 매핑 대상 테이블
    #     tbl_1 = preprocessing.integ_tbl(self.db, tables_1, columns)
    #     tbl_1.to_csv(tbl_cache + '/tbl_1.csv', index=False)
        
        
    # def title_preprocess(self):
    #     ''' 상품명 전처리 '''    

    #     tbl_0 = pd.read_csv(tbl_cache + '/tbl_0.csv')
    #     # deprepro_0 = preprocessing.title_preprocessor(tbl_0)
    #     deprepro_0 = ThreadPreprocess().title_preprocessor(tbl_0)
    #     deprepro_0.to_csv(tbl_cache + '/deprepro_0.csv', index=False)
        
    #     tbl_1 = pd.read_csv(tbl_cache + '/tbl_1.csv')
    #     # deprepro_1 = preprocessing.title_preprocessor(tbl_1)
    #     deprepro_1 = ThreadPreprocess().title_preprocessor(tbl_1)
    #     deprepro_1.to_csv(tbl_cache + '/deprepro_1.csv', index=False)
        
    # def categ_reclassify(self):
    #     ''' 카테고리 재분류 '''
        
    #     deprepro_0 = pd.read_csv(tbl_cache + '/deprepro_0.csv')
    #     deprepro_categ_0 = preprocessing.reclassifier(deprepro_0, 0)
    #     deprepro_categ_0.to_csv(tbl_cache + '/deprepro_categ_0.csv', index=False)
        
    #     deprepro_1 = pd.read_csv(tbl_cache + '/deprepro_1.csv')
    #     deprepro_categ_1 = preprocessing.reclassifier(deprepro_1, 1)
    #     deprepro_categ_1.to_csv(tbl_cache + '/deprepro_categ_1.csv', index=False)
        
    def mapping_prd(self):
        ''' 상품 군집화 및 타이틀 유사도 계산 '''
        
        # deprepro_categ_0 = pd.read_csv(tbl_cache + '/deprepro_0.csv')
        # deprepro_categ_1 = pd.read_csv(tbl_cache + '/deprepro_1.csv')
        # mapping_product.prd_mapper(deprepro_categ_0, deprepro_categ_1)
        
        compared_prds = mapping_product.prd_mapper()
        compared_prds.to_csv(tbl_cache + '/compared_prds.csv', index=False)
        
        # compared_prds = mapping_product.prd_mapper(deprepro_categ_0, deprepro_categ_1)
        # compared_prds.to_csv(tbl_cache + '/compared_prds.csv', index=False)
        
        
    def select_mapped_prd(self):
        ''' select mapped product '''
        
        compared_prds = pd.read_csv(tbl_cache + '/compared_prds.csv')
        
        mapped_prds = mapping_product.select_mapped_prd(compared_prds)
        mapped_prds.to_csv(tbl_cache + '/mapped_prds.csv', index=False)
        
        mapping_table = mapping_product.md_map_tbl(mapped_prds)
        mapping_table.to_csv(tbl_cache + '/mapping_table.csv', index=False)
        
    

        
        
        
        
        
        
        
        
        
        

        
        
        
        
        
class MainWidget(QWidget):
    ''' Database Connect Form '''
    
    def __init__(self):
        super().__init__()
        self.w0 = None
        self.w1 = None
        self.setWindowTitle('Connect Database')
        self.resize(475, 250)

        layout = QGridLayout()

        #
        label_name = QLabel('<font size="4"> Username </font>')
        self.lineEdit_username = QLineEdit()
        self.lineEdit_username.setPlaceholderText('Please enter your username')
        layout.addWidget(label_name, 0, 0)
        layout.addWidget(self.lineEdit_username, 0, 1)

        #
        label_password = QLabel('<font size="4"> Password </font>')
        self.lineEdit_password = QLineEdit()
        self.lineEdit_password.setPlaceholderText('Please enter your password')
        layout.addWidget(label_password, 1, 0)
        layout.addWidget(self.lineEdit_password, 1, 1)


        #
        label_database = QLabel('<font size="4"> Database </font>')
        self.lineEdit_database = QLineEdit()
        self.lineEdit_database.setPlaceholderText('Please enter database')
        layout.addWidget(label_database, 2, 0)
        layout.addWidget(self.lineEdit_database, 2, 1)
        
        #
        self.comb_menu = QComboBox()
        layout.addWidget(self.comb_menu, 3, 1)
        self.comb_menu.addItem('Mapping Products')
        self.comb_menu.addItem('Crawling')
        self.comb_menu.addItem('Get Table from Database')
        self.comb_menu.addItem('Upload Table to Database')
        self.comb_menu.move(50, 50)
        
        #
        btn_conn = QPushButton('Connect')
        btn_conn.clicked.connect(self.check_info)
        layout.addWidget(btn_conn, 5, 0, 1, 2)
        layout.setRowMinimumHeight(3, 75)

        self.setLayout(layout)
        
    # Select Menu 
    def select_menu(self):
        ''' 메뉴 선택 함수 '''
        
        menu_index = self.comb_menu.currentIndex()
        return menu_index
        
    def check_info(self):
        ''' db 연결 정보 확인 함수 '''
        
        msg = QMessageBox()
        conn = {
            0: self.lineEdit_username.text(),
            1: self.lineEdit_password.text(),
            2: self.lineEdit_database.text(),
        }
        
        db = access_db.AccessDataBase(conn[0], conn[1], conn[2])
        try:
            db.db_connect()
            msg.setText(f'Database Connection Successful! \n - {self.lineEdit_database.text()}')
            msg.exec_()
            
            # save connect info 
            with open(cur_dir + '/conn.txt', 'wb') as f:
                pickle.dump(conn, f)
            
            menu_index = self.select_menu()
            if menu_index == 0:
                if self.w0 is None:
                    self.w0 = MappingWindow()
                else:
                    self.w0.close()
                    self.w0 = MappingWindow()
                self.w0.show()
                
                
            elif menu_index == 1:
                if self.w1 is None:
                    self.w1 = CrawlingWindow()
                else:
                    self.w1.close()
                    self.w1 = CrawlingWindow()
                self.w1.show()
                
                
        except Exception as e:
            msg.setText(f'{e}')
            menu_index = self.select_menu()
            if menu_index == 0:
                if self.w0 is None:
                    self.w0 = MappingWindow()
                self.w0.show()
                
            msg.exec_()
            
            
            
def exec_gui():
    ''' main gui execution '''
    app = QApplication(sys.argv)
    form = MainWidget()
    form.show()
    sys.exit(app.exec_())
        
    
if __name__ == '__main__':
    app = QApplication(sys.argv)
    form = MainWidget()
    form.show()
    sys.exit(app.exec_())