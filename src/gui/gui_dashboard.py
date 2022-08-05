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
        self.data = self.db.get_tbl('beauty_kr_data_dashboard')
        
        # display counting products
        self.counting_products()
        
        # get data filtered & connect filter button
        self.get_data_filterd()
        self.select_filter.clicked.connect(self.get_data_filterd)
        self.reset_filter.clicked.connect(self.initFilter)
        self.reset_filter.clicked.connect(self.get_data_filterd)
        
        # init Combo Box
        self.initCombox()
        
        # # config plt
        # self.plt = plt
        # font = {
        #     'family' : 'AppleGothic',
        #     'weight' : 'bold',
        #     'size'   : 8,
        # }
        # self.plt.rc('font', **font)
        # self.plt.rcParams['figure.autolayout'] = True
        
        # init Ui
        self.initUI_category()
        self.initUI_brand()
        
        # connect button
        self.select_brand.clicked.connect(self.initUI_category)
        self.select_category.clicked.connect(self.initUI_brand)
        
    def initUI_category(self):
        ''' init UI (category) '''

        # config
        self.plt = plt
        font = {
            'family' : 'AppleGothic',
            'weight' : 'bold',
            'size'   : 8,
        }
        self.plt.rc('font', **font)
        self.plt.rcParams['figure.autolayout'] = True
        
        # groub by category 
        brand = self._select_brand()
        if brand == 'all brands':
            select_dict = None
        else:
            select_dict = {'brand_name': brand}
        
        ax, canvas, group_count_df = self.visualizer('category', select_dict)
        self.graph_category.takeAt(0)
        self.graph_category.addWidget(canvas)
        canvas.draw()
        
    def initUI_brand(self):
        ''' init UI (brand) '''

        # config plt
        self.plt = plt
        font = {
            'family' : 'AppleGothic',
            'weight' : 'bold',
            'size'   : 6.5,
        }
        self.plt.rc('font', **font)
        self.plt.rcParams['figure.autolayout'] = True
        
        # groub by brand 
        category = self._select_category()
        if category == 'all categories':
            select_dict = None
        else:
            select_dict = {'category': category}
        
        ax, canvas, group_count_df = self.visualizer('brand_name', select_dict, min_length=20)
        self.graph_brand.takeAt(0)
        self.graph_brand.addWidget(canvas)
        canvas.draw()
    
    def initFilter(self):
        ''' init filter (check box) '''
        
        self.check_mapping.setChecked(False)
        self.check_mapping.toggled.connect(self.filter_toggled)
        
        self.check_available.setChecked(False)
        self.check_available.toggled.connect(self.filter_toggled)
        
        self.check_review.setChecked(False)
        self.check_review.toggled.connect(self.filter_toggled)

    def counting_products(self):
        ''' Counting Products & display '''
        
        products_count_all = len(self.data)
        products_count_mapped = len(self.data[self.data.mapping_status==1])
        products_count_available = len(self.data[self.data.available_status==1])
        products_count_review = len(self.data[self.data.review_count.notnull()])

        self.All_Products.display(products_count_all)
        self.Mapping_Products.display(products_count_mapped)
        self.Available_Products.display(products_count_available)
        self.Review_Products.display(products_count_review)
        
    def filter_toggled(self):
        ''' Check filter option '''
        
        if self.check_mapping.isChecked():
            mapping_status = True
        else:
            mapping_status = False
        if self.check_available.isChecked():
            available_status = True
        else:
            available_status = False
        if self.check_review.isChecked():
            review_status = True
        else:
            review_status = False
                
        return mapping_status, available_status, review_status

    def initCombox(self):
        ''' init combo box '''
        
        grp_data = self.grouping_data('brand_name')
        brands = grp_data.index
        self.brands.addItem('all brands')
        for brand in brands:
            self.brands.addItem(brand)
            
        grp_data = self.grouping_data('category')
        categories = grp_data.index
        self.categories.addItem('all categories')
        for category in categories:
            self.categories.addItem(category)
        
    def get_data_filterd(self):
        ''' Apply filter '''
        
        self.data_filtered = self.data.copy()
        
        # check filter
        mapping_status, available_status, review_status = self.filter_toggled()
        if mapping_status:
            self.data_filtered = self.data_filtered[self.data_filtered.mapping_status==1]
        if available_status:
            self.data_filtered = self.data_filtered[self.data_filtered.available_status==1]
        if review_status:
            self.data_filtered = self.data_filtered[self.data_filtered.review_count.notnull()]
        
        self.data_filtered = self.data_filtered.reset_index(drop=True)
        
    def grouping_data(self, group_by, selector=None):
        
        if selector is None:
            _data_filtered = self.data_filtered.copy()
        else:
            col = list(selector.keys())[0]
            val = selector[col]
            _data_filtered = self.data_filtered[self.data_filtered[col]==val].reset_index(drop=True)
        
        # grouping data
        cnt_df = _data_filtered.groupby(group_by).count()
        group_count_df = pd.DataFrame(cnt_df.item_key).rename(columns={'item_key': 'product_counts'}).sort_values(by='product_counts', ascending=False)

        return group_count_df
    
    def visualizer(self, group_by, selector, min_length=None):
            
        group_count_df = self.grouping_data(group_by, selector)
        
        # min length
        if min_length is None:
            _group_count_df = group_count_df.copy()
        else:
            _group_count_df = group_count_df.iloc[:min_length]
            
        canvas = FigureCanvas(self.plt.Figure())
        ax = canvas.figure.subplots()

        # visualization bar plot
        sns.barplot(ax=ax, x="product_counts", y=_group_count_df.index, data=_group_count_df)
        
        # calculate sum
        group_count_df.loc['Sum', 'product_counts'] = group_count_df.product_counts.sum()

        return ax, canvas, group_count_df
    
    def _select_brand(self):
        ''' select brand '''
        
        brand = self.brands.currentText()
        return brand
    
    def _select_category(self):
        ''' select category '''
        
        category = self.categories.currentText()
        return category
        
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = DashboardWindow()
    window.show()
    window.showFullScreen()
    app.exec_()