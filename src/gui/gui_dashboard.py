import re
import os
import sys
import pickle
import pandas as pd

# Visualizations
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas

# Pyqt5
from PyQt5 import uic
from PyQt5.QtWidgets import QMainWindow, QMessageBox, QFileDialog, QListWidgetItem, QVBoxLayout
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QApplication


if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
    form_dir = os.path.join(root, 'form')
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src = os.path.abspath(os.path.join(cur_dir, os.pardir))
    sys.path.append(src)
    form_dir = os.path.join(src, 'gui/form')
    
tbl_cache = os.path.join(root, 'tbl_cache')
conn_path = os.path.join(root, 'conn.txt')
form_path = os.path.join(form_dir, 'dashboardWindow.ui')
form = uic.loadUiType(form_path)[0]

from access_database.access_db import AccessDataBase
from multithreading.thread_mapping import ThreadTitlePreprocess, ThreadMapping
from gui.table_view import TableViewer
from mapping._preprocessing import TitlePreProcess

class DashboardWindow(QMainWindow, form):
    ''' Product Mapping Window '''
    
    def __init__(self):
        super().__init__()    
        self.setupUi(self)
        self.setWindowTitle('Data Dashboard')
        
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = AccessDataBase(conn[0], conn[1], conn[2])
        
        # category reclassify
        self.tp = TitlePreProcess()
        
        # get data
        self.get_data()
        
        # initUi
        self.initUI()
        
    def initUI(self):

        self.plt = plt
        # self.plt.figure(figsize = (25, 17.5))
        self.plt.rcParams['font.family'] = 'AppleGothic'
        
        canvas, group_count_df = self.visualizer_grouping_data('category')
        self.categoryLayout.addWidget(canvas)
        canvas.draw()
        
        canvas, group_count_df = self.visualizer_grouping_data('brand_name')
        self.brandLayout.addWidget(canvas)
        canvas.draw()
        
    def get_data(self):
        gl_info = self.db.integ_tbl(['glowpick_product_info_final_version'], ['id', 'product_code', 'brand_name', 'selection', 'division', 'groups', 'dup_check'])
        gl_info_categ = self.tp.categ_reclassifier(gl_info)
        self.gl_info_dedup = gl_info_categ[gl_info_categ.dup_check!=-1]
        
    def visualizer_grouping_data(self, group_by):
        
        canvas = FigureCanvas(self.plt.Figure())
        ax = canvas.figure.subplots()
        
        cnt_df = self.gl_info_dedup.groupby(group_by).count()
        group_count_df = pd.DataFrame(cnt_df.id).rename(columns={'id': 'product_counts'}).sort_values(by='product_counts', ascending=False)
        
        # max index: 25
        _group_count_df = group_count_df.iloc[:10]
        sns.barplot(ax=ax, x="product_counts", y=_group_count_df.index, data=_group_count_df)

        return canvas, group_count_df
    
    
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = DashboardWindow()
    window.show()
    app.exec_()
        
        
        