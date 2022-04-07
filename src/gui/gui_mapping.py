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
from mapping import preprocessing
from mapping.preprocessing import ThreadTitlePreprocess
from mapping import mapping_product
from mapping.mapping_product import ThreadComparing

from gui.get_table import GetDialog
from gui.table_view import TableViewer

import sys
from PyQt5 import uic
from PyQt5 import QtCore
from PyQt5.QtWidgets import QMainWindow, QMessageBox, QFileDialog


if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    base_path = sys._MEIPASS
    tbl_cache = os.path.join(base_path, 'tbl_cache_')
    
else:
    base_path = os.path.dirname(os.path.realpath(__file__))
    tbl_cache = os.path.join(root, 'tbl_cache')

conn_path = os.path.join(base_path, 'conn.txt')
form_path = os.path.join(base_path, 'form/mappingWindow.ui')

mapping_form = uic.loadUiType(form_path)[0]

class MappingWindow(QMainWindow, mapping_form):
    ''' Product Mapping Window '''
    
    def __init__(self):
        super().__init__()    
        self.setupUi(self)
        self.setWindowTitle('Mapping Product')
        self.textBrowser.setOpenExternalLinks(True)
        self.viewer = None
        
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = access_db.AccessDataBase(conn[0], conn[1], conn[2])
        
        # get table
        self.view_table_name.clicked.connect(self.connect_dialog)
        self.get_table.clicked.connect(self.get_tbl)
        self.view_table_0.clicked.connect(self._view_0)
        self.save_0.clicked.connect(self._save_0)
        
        # preprocessing
        self.thread_preprocess = ThreadTitlePreprocess()
        self.thread_preprocess.progress.connect(self.update_progress)
        self.preprocess.clicked.connect(self._preprocess)
        self.stop_preprocess.clicked.connect(self.thread_preprocess.stop)
        
        
        # comparing
        self.thread_compare = ThreadComparing()
        self.thread_compare.progress.connect(self._update_progress)
        self.compare.clicked.connect(self._comparing)
        self.stop_compare.clicked.connect(self.thread_compare.stop)
        
        
        # mapping
        self.mapping.clicked.connect(self.select_mapped_prd)
        
        
    def update_progress(self, progress):
        
        prg_dict = progress.format_dict
        itm = prg_dict['n'] 
        tot = prg_dict['total']
        per = round((itm / tot) * 100, 0)
        elapsed = int(round(prg_dict['elapsed'], 0))
        if itm >= 1:
            remain_time = int(round((elapsed * tot / itm) - elapsed, 0))
        else:
            remain_time = 0
        
        self.pbar_2.setValue(per)
        
        message = f"{int(per)}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed}s < Remain time: {remain_time}s "
        self.statusbar.showMessage(message)
        
    def _update_progress(self, progress):
        
        prg_dict = progress.format_dict
        itm = prg_dict['n'] 
        tot = prg_dict['total']
        per = round((itm / tot) * 100, 0)
        elapsed = round(prg_dict['elapsed'], 0)
        if itm >= 1:
            remain_time = round((elapsed * tot / itm) - elapsed, 0)
        else:
            remain_time = 0
        
        self.pbar_3.setValue(per)
        
        elapsed_h = int(elapsed // 3600)
        elapsed_m = int((elapsed % 3600) // 60)
        elapsed_s = int(elapsed - (elapsed_h * 3600 + elapsed_m * 60))
        
        remain_h = int(remain_time // 3600)
        remain_m = int((remain_time % 3600) // 60)
        remain_s = int(remain_time - (remain_h * 3600 + remain_m * 60))
        
        message = f"{int(per)}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed_h}:{elapsed_m}:{elapsed_s} < Remain time: {remain_h}:{remain_m}:{remain_s} "
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
        print(f'\n\n\n<tables>\n{tbl_}\n\n\n')
        
        if tbl_ == '':
            msg = QMessageBox()
            msg.setText(f'Please check the table')
            msg.exec_()
            
        else:
            tbls = tbl_.split('\n')
            tbl_1 = preprocessing.integ_tbl(self.db, tbls, columns)
            tbl_1.to_csv(tbl_cache + '/tbl_1.csv', index=False)
            
            msg = QMessageBox()
            msg.setText(f'Table import success')
            msg.exec_()
            
    def _preprocess(self):
        ''' 쓰레드 연결 및 전처리 수행 ''' 
        
        self.thread_preprocess.power = True
        self.thread_preprocess.start()
        
    def _comparing(self):
        ''' 쓰레드 연결 및 상품정보 비교 수행 '''
        
        self.thread_compare.power = True
        self.thread_compare.start()
        
    def select_mapped_prd(self):
        ''' select mapped product '''
        
        compared_prds = pd.read_csv(tbl_cache + '/compared_prds.csv')
        
        mapped_prds = mapping_product.select_mapped_prd(compared_prds)
        mapped_prds.to_csv(tbl_cache + '/mapped_prds.csv', index=False)
        
        mapping_table = mapping_product.md_map_tbl(mapped_prds)
        mapping_table.to_csv(tbl_cache + '/mapping_table.csv', index=False)
        
        
    def save_file(self, file_name):
        ''' 파일 저장하기 '''
        
        file_path = os.path.join(tbl_cache, file_name)
        df = pd.read_csv(file_path)
        
        # save_path = os.path.join(root, file_name)
        file_save = QFileDialog.getSaveFileName(self, "Save File", "", "csv file (*.csv)")
        
        if file_save[0] != "":
            df.to_csv(file_save[0])
            
            
    def _save_0(self):
        file_name = "tbl_1.csv"
        self.save_file(file_name)
        
        
    def _view_0(self):
        file_name = "tbl_1.csv"
        # file_path = os.path.join(tbl_cache, file_name)
        # df = pd.read_csv(file_path)
    
        if self.viewer is None:
            self.viewer = TableViewer()
        else:
            self.viewer.close()
            self.viewer = TableViewer()
            
        self.viewer.show()
        self.viewer._loadFile(file_name)
        
        
        
        
             
    
# class DataFrameViewer(QtCore.QAbstractTableModel):
#     DtypeRole = QtCore.Qt.UserRole + 1000
#     ValueRole = QtCore.Qt.UserRole + 1001

#     def __init__(self, df=pd.DataFrame(), parent=None):
#         super(DataFrameViewer, self).__init__(parent)
#         self._dataframe = df

#     def setDataFrame(self, dataframe):
#         self.beginResetModel()
#         self._dataframe = dataframe.copy()
#         self.endResetModel()

#     def dataFrame(self):
#         return self._dataframe

#     dataFrame = QtCore.pyqtProperty(pd.DataFrame, fget=dataFrame, fset=setDataFrame)

#     @QtCore.pyqtSlot(int, QtCore.Qt.Orientation, result=str)
#     def headerData(self, section: int, orientation: QtCore.Qt.Orientation, role: int = QtCore.Qt.DisplayRole):
#         if role == QtCore.Qt.DisplayRole:
#             if orientation == QtCore.Qt.Horizontal:
#                 return self._dataframe.columns[section]
#             else:
#                 return str(self._dataframe.index[section])
#         return QtCore.QVariant()

#     def rowCount(self, parent=QtCore.QModelIndex()):
#         if parent.isValid():
#             return 0
#         return len(self._dataframe.index)

#     def columnCount(self, parent=QtCore.QModelIndex()):
#         if parent.isValid():
#             return 0
#         return self._dataframe.columns.size

#     def data(self, index, role=QtCore.Qt.DisplayRole):
#         if not index.isValid() or not (0 <= index.row() < self.rowCount() \
#             and 0 <= index.column() < self.columnCount()):
#             return QtCore.QVariant()
#         row = self._dataframe.index[index.row()]
#         col = self._dataframe.columns[index.column()]
#         dt = self._dataframe[col].dtype

#         val = self._dataframe.iloc[row][col]
#         if role == QtCore.Qt.DisplayRole:
#             return str(val)
#         elif role == DataFrameViewer.ValueRole:
#             return val
#         if role == DataFrameViewer.DtypeRole:
#             return dt
#         return QtCore.QVariant()

#     def roleNames(self):
#         roles = {
#             QtCore.Qt.DisplayRole: b'display',
#             DataFrameViewer.DtypeRole: b'dtype',
#             DataFrameViewer.ValueRole: b'value'
#         }
#         return roles
        
        
    