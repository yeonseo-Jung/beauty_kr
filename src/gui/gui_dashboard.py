import re
import os
import sys
import pickle
import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd

# Visualizations
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas

# Pyqt5
from PyQt5 import uic
from PyQt5.QtWidgets import QMainWindow, QMessageBox, QFileDialog
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
        self.viewer = None
        
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

        # get data filtered & connect filter button -> init ui
        # select
        self.get_data_filterd()
        self.select_filter.clicked.connect(self.get_data_filterd)
        self.select_filter.clicked.connect(self.initUI_category)
        self.select_filter.clicked.connect(self.initUI_brand)
        self.select_filter.clicked.connect(self.initUI_review)
        # reset
        self.reset_filter.clicked.connect(self.initFilter)
        self.reset_filter.clicked.connect(self.get_data_filterd)
        self.reset_filter.clicked.connect(self.initUI_category)
        self.reset_filter.clicked.connect(self.initUI_brand)
        self.reset_filter.clicked.connect(self.initUI_review)
        
        # init Combo Box
        self.initCombox(brand=True, category=True, brand_review=True)
        
        # plt config
        self.plt = plt
        font = {
            'family' : 'AppleGothic',
            'weight' : 'bold',
            'size'   : 7.5,
        }
        self.plt.rc('font', **font)
        self.plt.rcParams['figure.autolayout'] = True
        
        # init Ui
        self.initUI_category()
        self.initUI_brand()
        self.initUI_review()
        
        # connect button
        self.select_brand.clicked.connect(self.initUI_category)
        self.select_category.clicked.connect(self.initUI_brand)
        self.select_review.clicked.connect(self.initUI_review)
        
        self.view_category.clicked.connect(lambda: self.tbl_viewer(self.category_group_count_df))
        self.view_brand.clicked.connect(lambda: self.tbl_viewer(self.brand_group_count_df))
        self.view_review.clicked.connect(lambda: self.tbl_viewer(self.review_group_count_df))
        
        self.save_category.clicked.connect(lambda: self.save_file(self.category_group_count_df))
        self.save_brand.clicked.connect(lambda: self.save_file(self.brand_group_count_df))
        self.save_review.clicked.connect(lambda: self.save_file(self.review_group_count_df))
        
    def initUI_category(self):
        ''' init UI (category) '''

        # # config
        # self.plt = plt
        # font = {
        #     'family' : 'AppleGothic',
        #     'weight' : 'bold',
        #     'size'   : 8,
        # }
        # self.plt.rc('font', **font)
        # self.plt.rcParams['figure.autolayout'] = True
        
        # groub by category 
        group_by = 'category'
        brand = self._select_brand()
        if brand == 'all brands':
            select_dict = None
        else:
            select_dict = {'brand_name': brand}
        
        ax, canvas, group_count_df, status = self.visualizer(group_by, select_dict)
        self.graph_category.takeAt(0)
        if status == 0:
            msg = QMessageBox()
            msg.setText("** Empty Data **")
            msg.exec_()
            self.initCombox(brand=True)
            ax, canvas, group_count_df, status = self.visualizer(group_by, None)
        
        self.graph_category.addWidget(canvas)
        canvas.draw()
        
        d = {
            group_by: group_count_df.index,
            group_count_df.columns[0]: group_count_df.iloc[:, 0].values.astype('int')
        }
        self.category_group_count_df = pd.DataFrame(d)
        
    def initUI_brand(self):
        ''' init UI (brand) '''

        # # config plt
        # self.plt = plt
        # font = {
        #     'family' : 'AppleGothic',
        #     'weight' : 'bold',
        #     'size'   : 6.5,
        # }
        # self.plt.rc('font', **font)
        # self.plt.tick_params(left = False)
        # self.plt.rcParams['figure.autolayout'] = True
        
        # groub by brand 
        group_by = 'brand_name'
        category = self._select_category()
        if category == 'all categories':
            select_dict = None
        else:
            select_dict = {'category': category}
        
        ax, canvas, group_count_df, status = self.visualizer(group_by, select_dict, min_length=20)
        
        self.graph_brand.takeAt(0)
        if status == 0:
            msg = QMessageBox()
            msg.setText("** Empty Data **")
            msg.exec_()
            self.initCombox(category=True)
            ax, canvas, group_count_df, status = self.visualizer(group_by, None, min_length=20)
            
        self.graph_brand.addWidget(canvas)
        canvas.draw()
        
        d = {
            group_by: group_count_df.index,
            group_count_df.columns[0]: group_count_df.iloc[:, 0].values.astype('int')
        }
        self.brand_group_count_df = pd.DataFrame(d).astype({})
        
    def initUI_review(self):
        
        # # config plt
        # self.plt = plt
        # font = {
        #     'family' : 'AppleGothic',
        #     'weight' : 'bold',
        #     'size'   : 6.5,
        # }
        # self.plt.rc('font', **font)
        # self.plt.rcParams['figure.autolayout'] = True
        
        # groub by category 
        group_by = 'category'
        brand = self._select_brand_review()
        if brand == 'all brands':
            select_dict = None
        else:
            select_dict = {'brand_name': brand}
            
        ax, canvas, group_count_df, status = self.visualizer(group_by, select_dict, review=True)
        self.graph_review.takeAt(0)
        if status == 0:
            msg = QMessageBox()
            msg.setText("** Empty Data **")
            msg.exec_()
            self.initCombox(brand_review=True)
            ax, canvas, group_count_df, status = self.visualizer(group_by, None, review=True)
        
        self.graph_review.addWidget(canvas)
        canvas.draw()
        
        self.review_group_count_df = group_count_df.copy()
        
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
        
        # products
        products_count_all = len(self.data)
        products_count_mapped = len(self.data[self.data.mapping_status==1])
        products_count_available = len(self.data[self.data.available_status==1])
        self.All_Products.display(products_count_all)
        self.Mapping_Products.display(products_count_mapped)
        self.Available_Products.display(products_count_available)
        
        # reviews
        reviews_all = self.data.review_count.sum()
        reviews_mapped = self.data.loc[self.data.mapping_status==1, 'review_count'].sum()
        reviews_available = self.data.loc[self.data.available_status==1, 'review_count'].sum()
        self.All_Reviews.display(reviews_all)
        self.Mapping_Reviews.display(reviews_mapped)
        self.Available_Reviews.display(reviews_available)
        
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

    def initCombox(self, brand=False, category=False, brand_review=False):
        ''' init combo box '''
        
        if brand:
            grp_data = self.grouping_data('brand_name')
            brands = grp_data.index
            self.brands.clear()
            self.brands.addItem('all brands')
            for brand in brands:
                self.brands.addItem(brand)
        
        if category:
            grp_data = self.grouping_data('category')
            categories = grp_data.index
            self.categories.clear()
            self.categories.addItem('all categories')
            for category in categories:
                self.categories.addItem(category)
                
        if brand_review:
            grp_data = self.grouping_data('brand_name')
            brands = grp_data.index
            self.brands_review.clear()
            self.brands_review.addItem('all brands')
            for brand in brands:
                self.brands_review.addItem(brand)
        
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
        group_count_df = pd.DataFrame(cnt_df.item_key).rename(columns={'item_key': 'Product_Counts'}).sort_values(by='Product_Counts', ascending=False)

        return group_count_df
    
    def grouping_review(self, group_by, selector):
        
        if selector is None:
            _data_filtered = self.data_filtered.copy()
        else:
            col = list(selector.keys())[0]
            val = selector[col]
            _data_filtered = self.data_filtered[self.data_filtered[col]==val].reset_index(drop=True)
        
        review_count_dict = {}
        for group in _data_filtered[group_by].unique():
            reviews = int(_data_filtered.loc[_data_filtered[group_by]==group, 'review_count'].sum())
            if reviews == 0:
                continue
            review_count_dict[group] = reviews
            
        grouping_review_df = pd.DataFrame(columns=[group_by, 'Review_Counts'])
        grouping_review_df.iloc[:, 0] = review_count_dict.keys()
        grouping_review_df.iloc[:, 1] = review_count_dict.values()
        grouping_review_df = grouping_review_df.sort_values(by='Review_Counts', ascending=False, ignore_index=True)
        
        return grouping_review_df
    
    def visualizer(self, group_by, selector, min_length=None, review=False):
        
        if review:
            group_count_df = self.grouping_review(group_by, selector)
            _count = "Review_Counts"
        else:   
            group_count_df = self.grouping_data(group_by, selector)
            _count = "Product_Counts"
            
        # min length
        if min_length is None:
            _group_count_df = group_count_df.copy()
        else:
            _group_count_df = group_count_df.iloc[:min_length]
            
        canvas = FigureCanvas(self.plt.Figure())
        ax = canvas.figure.subplots()
        
        # check empty
        if _group_count_df.empty:
            status = 0
        else:
            status = 1
            # visualization bar plot & calculate total count
            if review:
                sns.barplot(ax=ax, x=_count, y=group_by, data=_group_count_df)
                group_count_df.loc[len(group_count_df)] = 'Total', group_count_df[_count].sum()
            else:
                sns.barplot(ax=ax, x=_count, y=_group_count_df.index, data=_group_count_df)
                group_count_df.loc['Total', _count] = group_count_df[_count].sum()

        return ax, canvas, group_count_df, status
    
    def _select_brand(self):
        ''' select brand '''
        
        brand = self.brands.currentText()
        return brand
    
    def _select_category(self):
        ''' select category '''
        
        category = self.categories.currentText()
        return category

    def _select_brand_review(self):
        ''' select brand (review) '''
        
        brand = self.brands_review.currentText()
        return brand
    
    def tbl_viewer(self, df):
        ''' table viewer '''
        
        if df is None:
            pass
        else:
            if self.viewer is None:
                self.viewer = TableViewer()
            else:
                self.viewer.close()
                self.viewer = TableViewer()
                
        self.viewer.show()
        self.viewer._loadTable(df)
        
    def save_file(self, df):
            ''' save csv file '''
            
            # 캐시에 해당 파일이 존재할 때 저장
            if df is None:
                msg = QMessageBox()
                msg.setText('일시정지 후 다시 시도해주세요')
                msg.exec_()
            else:
                file_save = QFileDialog.getSaveFileName(self, "Save File", "", "csv file (*.csv)")
                
                if file_save[0] != "":
                    df.to_csv(file_save[0], index=False)
        
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = DashboardWindow()
    window.show()
    window.showFullScreen()
    app.exec_()