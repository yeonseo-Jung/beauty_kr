import os
# import re
import sys
# import time
import pickle
import pandas as pd
# from tqdm.auto import tqdm

cur_dir = os.path.dirname(os.path.realpath(__file__))
root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
src = os.path.abspath(os.path.join(cur_dir, os.pardir))
sys.path.append(root)
sys.path.append(src)

from access_database import access_db
# from mapping import preprocessing
# from mapping.preprocessing import ThreadTitlePreprocess
# from mapping import mapping_product
# from mapping.mapping_product import ThreadComparing

# from gui.get_table import GetDialog
from gui.gui_scraping import CrawlingWindow
from gui.gui_mapping import MappingWindow


import sys
# from PyQt5 import uic
# from PyQt5 import QtCore
from PyQt5.QtWidgets import QWidget, QGridLayout, QLabel, QLineEdit, QComboBox, QPushButton, QMessageBox


if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    base_path = sys._MEIPASS
    tbl_cache = os.path.join(base_path, 'tbl_cache_')
    
else:
    base_path = os.path.dirname(os.path.realpath(__file__))
    tbl_cache = os.path.join(root, 'tbl_cache')

conn_path = os.path.join(base_path, 'conn.txt')

# form_path = os.path.join(base_path, 'form/mappingWindow.ui')

# mapping_form = uic.loadUiType(form_path)[0]

# class MappingWindow(QMainWindow, mapping_form):
#     ''' Product Mapping Window '''
    
#     def __init__(self):
#         super().__init__()    
#         self.setupUi(self)
#         self.setWindowTitle('Mapping Product')
        
#         self.textBrowser.setOpenExternalLinks(True)
        
#         # db 연결
#         with open(conn_path, 'rb') as f:
#             conn = pickle.load(f)
#         self.db = access_db.AccessDataBase(conn[0], conn[1], conn[2])
        
#         self.view_table_name.clicked.connect(self.connect_dialog)
#         self.get_table.clicked.connect(self.get_tbl)
        
#         # preprocessing
#         self.thread_preprocess = ThreadTitlePreprocess()
#         self.thread_preprocess.progress.connect(self.update_progress)
#         self.preprocess.clicked.connect(self._preprocess)
#         self.stop_preprocess.clicked.connect(self.thread_preprocess.stop)
        
        
#         # comparing
#         self.thread_compare = ThreadComparing()
#         self.thread_compare.progress.connect(self._update_progress)
#         self.compare.clicked.connect(self._comparing)
#         self.stop_compare.clicked.connect(self.thread_compare.stop)
        
        
#         # mapping
#         self.mapping.clicked.connect(self.select_mapped_prd)
        
        
#     def update_progress(self, progress):
        
#         prg_dict = progress.format_dict
#         itm = prg_dict['n'] 
#         tot = prg_dict['total']
#         per = round((itm / tot) * 100, 0)
#         elapsed = int(round(prg_dict['elapsed'], 0))
#         if itm >= 1:
#             remain_time = int(round((elapsed * tot / itm) - elapsed, 0))
#         else:
#             remain_time = 0
        
#         self.pbar_2.setValue(per)
        
#         message = f"{int(per)}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed}s < Remain time: {remain_time}s "
#         self.statusbar.showMessage(message)
        
#     def _update_progress(self, progress):
        
#         prg_dict = progress.format_dict
#         itm = prg_dict['n'] 
#         tot = prg_dict['total']
#         per = round((itm / tot) * 100, 0)
#         elapsed = round(prg_dict['elapsed'], 0)
#         if itm >= 1:
#             remain_time = round((elapsed * tot / itm) - elapsed, 0)
#         else:
#             remain_time = 0
        
#         self.pbar_3.setValue(per)
        
#         elapsed_h = int(elapsed // 3600)
#         elapsed_m = int((elapsed % 3600) // 60)
#         elapsed_s = int(elapsed - (elapsed_h * 3600 + elapsed_m * 60))
        
#         remain_h = int(remain_time // 3600)
#         remain_m = int((remain_time % 3600) // 60)
#         remain_s = int(remain_time - (remain_h * 3600 + remain_m * 60))
        
#         message = f"{int(per)}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed_h}:{elapsed_m}:{elapsed_s} < Remain time: {remain_h}:{remain_m}:{remain_s} "
#         self.statusbar.showMessage(message)
        
#     def connect_dialog(self):
#         ''' get.GetDialog connect '''    
        
#         self.get = GetDialog()
#         self.get.tables.connect(self.append_text)
#         self.get.show()
        
#     def append_text(self, tables):
#         self.textBrowser.clear()
#         self.textBrowser.append(tables)
        
        
#     def get_tbl(self):
#         ''' 데이터 베이스에서 테이블 가져와서 통합하기 '''
        
#         # 상품 매핑에 필요한 컬럼
#         columns = ['id', 'brand_name', 'product_name', 'selection', 'division', 'groups']
        
#         # 매핑 기준 테이블 
#         tbl_0 = self.db.get_tbl('glowpick_product_info_final_version', columns)
#         tbl_0.loc[:, 'table_name'] = 'glowpick_product_info_final_version'
#         tbl_0.to_csv(tbl_cache + '/tbl_0.csv', index=False)
        
#         # 매핑 대상 테이블
#         tbl_ = self.textBrowser.toPlainText()
#         print(f'\n\n\n<tables>\n{tbl_}\n\n\n')
        
#         if tbl_ == '':
#             msg = QMessageBox()
#             msg.setText(f'Please check the table')
#             msg.exec_()
            
#         else:
#             tbls = tbl_.split('\n')
#             tbl_1 = preprocessing.integ_tbl(self.db, tbls, columns)
#             tbl_1.to_csv(tbl_cache + '/tbl_1.csv', index=False)
            
#             msg = QMessageBox()
#             msg.setText(f'Table import success')
#             msg.exec_()
            
        
        
#     def _preprocess(self):
#         ''' 쓰레드 연결 및 전처리 수행 ''' 
        
#         self.thread_preprocess.power = True
#         self.thread_preprocess.start()
        
        
        
#     def _comparing(self):
#         ''' 쓰레드 연결 및 상품정보 비교 수행 '''
        
#         self.thread_compare.power = True
#         self.thread_compare.start()
        
    
#     def select_mapped_prd(self):
#         ''' select mapped product '''
        
#         compared_prds = pd.read_csv(tbl_cache + '/compared_prds.csv')
        
#         mapped_prds = mapping_product.select_mapped_prd(compared_prds)
#         mapped_prds.to_csv(tbl_cache + '/mapped_prds.csv', index=False)
        
#         mapping_table = mapping_product.md_map_tbl(mapped_prds)
#         mapping_table.to_csv(tbl_cache + '/mapping_table.csv', index=False)
        
        
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
            with open(conn_path, 'wb') as f:
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
            msg.exec_()
            # menu_index = self.select_menu()
            # if menu_index == 0:
            #     if self.w0 is None:
            #         self.w0 = MappingWindow()
            #     self.w0.show()
            
            
            
# def exec_gui():
#     ''' main gui execution '''
#     app = QApplication(sys.argv)
#     form = MainWidget()
#     form.show()
#     sys.exit(app.exec_())
        
    
# if __name__ == '__main__':
#     app = QApplication(sys.argv)
#     form = MainWidget()
#     form.show()
#     sys.exit(app.exec_())