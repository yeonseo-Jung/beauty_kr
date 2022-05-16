import os
import re
import sys
import pickle
import pandas as pd

from PyQt5 import uic
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QMainWindow, QMessageBox, QFileDialog, QListWidgetItem

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    base_path = sys._MEIPASS
    tbl_cache = os.path.join(base_path, 'tbl_cache_')
    
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src = os.path.abspath(os.path.join(cur_dir, os.pardir))
    sys.path.append(root)
    sys.path.append(src)
    base_path = os.path.dirname(os.path.realpath(__file__))
    tbl_cache = os.path.join(root, 'tbl_cache')
    
conn_path = os.path.join(base_path, 'conn.txt')
form_path = os.path.join(base_path, 'form/crawlingNvRevWindow.ui')

from access_database import access_db
from multithreading.thread_crawling import CrawlingNvRev
from gui.table_view import TableViewer
from scraping.scraper import ReviewScrapeNv

form = uic.loadUiType(form_path)[0]

class CrawlingNvRevWindow(QMainWindow, form):
    ''' Product Info Crawling Window '''
    
    def __init__(self):
        super().__init__()    
        self.setupUi(self)
        self.setWindowTitle('Crawling Naver Product Review')
        self.viewer = None
        self.scrape_df_name = "scrape_df.csv"
        self.path_scrape_df = os.path.join(tbl_cache, self.scrape_df_name)
        self.rs = ReviewScrapeNv()
        self.thread_crw = CrawlingNvRev()
        
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = access_db.AccessDataBase(conn[0], conn[1], conn[2])
        
        for table in self._get_tbl():
            item = QListWidgetItem(table)
            item.setCheckState(Qt.Unchecked)
            self.TableList.addItem(item)
        
        # connect thread
        self.thread_crw.progress.connect(self.update_progress)
        
        # connect func & btn
        self.Accept.clicked.connect(self._accept)
        self.Run.clicked.connect(self._run)
        self.Pause.clicked.connect(self.thread_crw.stop)
        self.View.clicked.connect(self.tbl_viewer)
        self.Save.clicked.connect(self.save_file)
            
    def _get_tbl(self):
        ''' db에서 네이버 상품 테이블 리스트 가져오기 '''
        
        tables = self.db.get_tbl_name()
        reg = re.compile('naver_beauty_product_info_extended_v[0-9]+')
        table_list = []
        for tbl in tables:
            tbl_ = re.match(reg, tbl)
            if tbl_:
                table_list.append(tbl_.group(0))
        table_list = sorted(list(set(table_list)))
        return table_list
        
    def _accept(self):
        ''' 선택된 테이블 가져오기 '''
        
        tbls = []
        for idx in range(self.TableList.count()):
            if self.TableList.item(idx).checkState() == Qt.Checked:
                tbls.append(self.TableList.item(idx).text())
                
        if len(tbls) == 0:
            msg = QMessageBox()
            msg.setText(f'리뷰 업데이트 할 테이블을 선택하세요')
            msg.exec_()
            
        elif len(tbls) >= 2:
            msg = QMessageBox()
            msg.setText(f'리뷰 업데이트 할 1개 테이블만 선택하세요')
            msg.exec_()
            
        else:
            table_name = tbls[0]
            columns = ['id', 'product_url', 'reviews_count', 'review_status']
            tbl = self.db.get_tbl(table_name, columns)
            
            tbl_ = tbl.loc[(tbl.review_status==1) | (tbl.review_status==0) | (tbl.review_status==-1)].reset_index(drop=True)
            _count = round(tbl_.reviews_count.sum() * 1.3 / 10000, 1)
            _time = round(600 * _count * 10000 / 7000 / 3600, 1)
            self.reviews_count.display(_count)
            self.time.display(_time)
            
            self.file_path = os.path.join(tbl_cache, 'df_for_rev_crw.csv')
            tbl_.loc[:, ['id', 'product_url']].to_csv(self.file_path, index=False)
            
            # status table upload
            df_status = pd.DataFrame(columns=['id', 'status'])
            df_status.loc[:, 'id'] = tbl_.id.tolist()
            df_status.loc[:, 'status'] = 0
            
            # upload status table
            self.status_table_name = f'{table_name}_status_temp'
            self.db.engine_upload(df_status, self.status_table_name , 'replace')
            # save status table name
            with open(tbl_cache + '/status_table_name.txt', 'wb') as f:
                pickle.dump([self.status_table_name], f)
            
            msg = QMessageBox()
            msg.setText(f'Table accept success')
            msg.exec_()
            
    def update_progress(self, progress):
        if os.path.isfile(tbl_cache + '/prg_dict.txt'):
            with open(tbl_cache + '/prg_dict.txt', 'rb') as f:
                prg_dict_ = pickle.load(f)
            itm_ = prg_dict_['n'] 
            elapsed_ = round(prg_dict_['elapsed'], 0)
            
        else:
            itm_, elapsed_ = 1, 0
        
        prg_dict = progress.format_dict
        itm = prg_dict['n'] + itm_
        tot = prg_dict['total'] + itm_ - 1
        per = round((itm / tot) * 100, 0)
        elapsed = round(prg_dict['elapsed'], 0) + elapsed_
        prg_dict_ = {
            'n': itm,
            'elapsed': elapsed,
        }
                
        if itm >= 1:
            remain_time = round((elapsed * tot / itm) - elapsed, 0)
        else:
            remain_time = 0
        
        self.progressBar.setValue(per)
        
        elapsed_h = int(elapsed // 3600)
        elapsed_m = int((elapsed % 3600) // 60)
        elapsed_s = int(elapsed - (elapsed_h * 3600 + elapsed_m * 60))
        
        remain_h = int(remain_time // 3600)
        remain_m = int((remain_time % 3600) // 60)
        remain_s = int(remain_time - (remain_h * 3600 + remain_m * 60))
        
        message = f"{int(per)}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed_h}:{elapsed_m}:{elapsed_s} < Remain time: {remain_h}:{remain_m}:{remain_s}"
        self.statusbar.showMessage(message)
        
        # pause 시에 현재까지 진행률 저장
        if self.thread_crw.power == False:
            with open(tbl_cache + '/prg_dict.txt', 'wb') as f:
                pickle.dump(prg_dict_, f)
                
            message = f"{int(per)}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed_h}:{elapsed_m}:{elapsed_s} < Remain time: {remain_h}:{remain_m}:{remain_s} **PAUSE**"
            self.statusbar.showMessage(message)
    
    def _run(self):
        if os.path.isfile(self.file_path):            
            msg = QMessageBox()
            msg.setText("- 인터넷 연결 확인 \n- VPN 연결 확인 \n- 자동 잠금 해제 확인")
            msg.exec_()
            self.thread_crw.power = True
            self.thread_crw.start()
        else:
            msg = QMessageBox()
            msg.setText("Accept 완료 후 시도하세요")
            msg.exec_()
        
    def save_file(self):
        ''' save csv file '''
        
        # 캐시에 해당 파일이 존재할 때 저장
        if os.path.isfile(self.path_scrape_df):
            df = pd.read_csv(self.path_scrape_df)
            file_save = QFileDialog.getSaveFileName(self, "Save File", "", "csv file (*.csv)")
            
            if file_save[0] != "":
                df.to_csv(file_save[0], index=False)
        else:
            msg = QMessageBox()
            msg.setText('일시정지 후 다시 시도해주세요')
            msg.exec_()
            
    def tbl_viewer(self):
        ''' table viewer '''
        
        # 캐시에 테이블이 존재할 때 open table viewer
        if os.path.isfile(self.path_scrape_df):
            if self.viewer is None:
                self.viewer = TableViewer()
            else:
                self.viewer.close()
                self.viewer = TableViewer()
                
            self.viewer.show()
            self.viewer._loadFile(self.scrape_df_name)
        else:
            msg = QMessageBox()
            msg.setText('일시정지 후 다시 시도해주세요')
            msg.exec_()