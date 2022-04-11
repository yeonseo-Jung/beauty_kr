import os
import sys
import pickle
import pandas as pd

cur_dir = os.path.dirname(os.path.realpath(__file__))
root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
src = os.path.abspath(os.path.join(cur_dir, os.pardir))
sys.path.append(root)
sys.path.append(src)

from access_database import access_db
from gui.gui_scraping import ScrapingWindow
from gui.gui_mapping import MappingWindow

from PyQt5.QtWidgets import QWidget, QGridLayout, QLabel, QLineEdit, QComboBox, QPushButton, QMessageBox


if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    base_path = sys._MEIPASS
    tbl_cache = os.path.join(base_path, 'tbl_cache_')
    
else:
    base_path = os.path.dirname(os.path.realpath(__file__))
    tbl_cache = os.path.join(root, 'tbl_cache')

conn_path = os.path.join(base_path, 'conn.txt')
        
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
        self.comb_menu.addItem('Scraping')
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
                    self.w1 = ScrapingWindow()
                else:
                    self.w1.close()
                    self.w1 = ScrapingWindow()
                self.w1.show()
                
                
        except Exception as e:
            msg.setText(f'{e}')
            msg.exec_()
            
            
            
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