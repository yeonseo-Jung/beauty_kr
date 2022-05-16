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
# tbl_cache = root + '/tbl_cache'
sys.path.append(root)
sys.path.append(src)
# sys.path.append(tbl_cache)

from access_database import access_db
from mapping import preprocessing
from mapping.preprocessing import ThreadTitlePreprocess
from mapping import mapping_product

# from gui import gui_main


import sys
from PyQt5 import uic
from PyQt5 import QtCore
from PyQt5.QtWidgets import *

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    base_path = sys._MEIPASS
    tbl_cache = os.path.join(base_path, 'tbl_cache_')
    
else:
    base_path = os.path.dirname(os.path.realpath(__file__))
    tbl_cache = os.path.join(root, 'tbl_cache')

form_path = os.path.join(base_path, 'form/get_table.ui')
get_form = uic.loadUiType(form_path)[0]

class GetDialog(QDialog, get_form):
    
    def __init__(self):
        super().__init__()
        self.setupUi(self)
        self.checkBox_1.setChecked(False)
        self.checkBox_1.toggled.connect(self.table_toggled)
        self.checkBox_2.setChecked(False)
        self.checkBox_2.toggled.connect(self.table_toggled)
        self.checkBox_3.setChecked(False)
        self.checkBox_3.toggled.connect(self.table_toggled)
        self.checkBox_4.setChecked(False)
        self.checkBox_4.toggled.connect(self.table_toggled)
        self.checkBox_5.setChecked(False)
        self.checkBox_5.toggled.connect(self.table_toggled)
        self.checkBox_6.setChecked(False)
        self.checkBox_6.toggled.connect(self.table_toggled)
        self.checkBox_7.setChecked(False)
        self.checkBox_7.toggled.connect(self.table_toggled)
        
        self.accept.clicked.connect(self.output_signal)
        

    def table_toggled(self):
        tbls = []
        tbl_ = ""
        
        if self.checkBox_1.isChecked():
            tbl = "naver_beauty_product_info_extended_v1_211217"
            tbls.append(tbl)
            
        if self.checkBox_2.isChecked():
            tbl = "naver_beauty_product_info_extended_v2_211231"
            tbls.append(tbl)
            
        if self.checkBox_3.isChecked():
            tbl = "naver_beauty_product_info_extended_v3_220124"
            tbls.append(tbl)
            
        if self.checkBox_4.isChecked():
            tbl = "naver_beauty_product_info_extended_v4_220311"
            tbls.append(tbl)
            
        if self.checkBox_5.isChecked():
            tbl = ""
            tbls.append(tbl)
            
        if self.checkBox_6.isChecked():
            tbl = ""
            tbls.append(tbl)
            
        if self.checkBox_7.isChecked():
            tbl = ""
            tbls.append(tbl)
        
        
        for i in range(len(tbls)):
            if i == len(tbls) - 1:
                tbl_ += tbls[i]
            else:
                tbl_  += tbls[i] + '\n'
        
        
        if len(tbls) == 0:
            tbl_ = "No Table"
        
        return tbl_
    
    tables = QtCore.pyqtSignal(str)
    def output_signal(self):
        tbl_ = self.table_toggled()
        self.tables.emit(tbl_)
        self.close()
        
    # def reject(self):
    #     self.close()
        
        
        
    