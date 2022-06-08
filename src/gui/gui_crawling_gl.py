import os
import re
import sys
import pickle
import pandas as pd
from tqdm.auto import tqdm

from PyQt5 import uic
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QMainWindow, QMessageBox, QFileDialog

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
form_path = os.path.join(base_path, 'form/crawlingGlInfoRevWindow.ui')

from access_database import access_db
from multithreading.thread_crawling_glowpick import ThreadCrawlingGl
from multithreading.thread_crawling_glowpick import ThreadCrawlingProductCode
from gui.table_view import TableViewer

form = uic.loadUiType(form_path)[0]
class CrawlingGlWindow(QMainWindow, form):
    ''' Product Info Crawling Window '''
    
    def __init__(self):
        super().__init__()    
        self.setupUi(self)
        self.setWindowTitle('Update Glowpick Products')
        self.viewer = None
        self.file_path = os.path.join(tbl_cache, 'product_codes.txt')
        self.selections = os.path.join(tbl_cache, 'selections.txt')
        self.divisions = os.path.join(tbl_cache, 'divisions.txt')
        self.path_scrape_df = os.path.join(tbl_cache, 'gl_info.csv')
        self.path_scrape_df_rev = os.path.join(tbl_cache, 'gl_info_rev.csv')
        
        # init class
        self.thread_crw = ThreadCrawlingGl()
        self.thread_code = ThreadCrawlingProductCode()
        
        # connect thread
        self.thread_crw.progress.connect(self.update_progress)
        self.thread_code.progress.connect(self._update_progress)
         
        # connect func & btn
        self.Select.clicked.connect(self._select)
        self.Run_2.clicked.connect(self._run_prd_codes)
        self.Run.clicked.connect(self._run_crawling)
        self.Stop.clicked.connect(self.thread_code.stop)
        self.Pause.clicked.connect(self.thread_crw.stop)
        self.View.clicked.connect(self.tbl_viewer)
        self.Save.clicked.connect(self.save_file)
        self.Upload.clicked.connect(self.thread_crw._upload_df)
        
        # connect db
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = access_db.AccessDataBase(conn[0], conn[1], conn[2])
        
        # category toggled
        self.skincare.setChecked(False)
        self.skincare.toggled.connect(self.categ_toggled)
        self.bodycare.setChecked(False)
        self.bodycare.toggled.connect(self.categ_toggled)
        self.makeup.setChecked(False)
        self.makeup.toggled.connect(self.categ_toggled)
        self.haircare.setChecked(False)
        self.haircare.toggled.connect(self.categ_toggled)
        self.cleansing.setChecked(False)
        self.cleansing.toggled.connect(self.categ_toggled)
        self.menscare.setChecked(False)
        self.menscare.toggled.connect(self.categ_toggled)
        self.suncare.setChecked(False)
        self.suncare.toggled.connect(self.categ_toggled)
        self.maskpack.setChecked(False)
        self.maskpack.toggled.connect(self.categ_toggled)
        
    def update_progress(self, progress):
        
        if os.path.isfile(tbl_cache + '/prg_dict.txt'):
            with open(tbl_cache + '/prg_dict.txt', 'rb') as f:
                prg_dict_ = pickle.load(f)
            itm_ = prg_dict_['n'] 
            elapsed_ = round(prg_dict_['elapsed'], 0)
            
        else:
            itm_, elapsed_ = 0, 0
        
        prg_dict = progress.format_dict
        itm = prg_dict['n'] + itm_
        tot = prg_dict['total'] + itm_ 
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
        if not self.thread_crw.power:
            with open(tbl_cache + '/prg_dict.txt', 'wb') as f:
                pickle.dump(prg_dict_, f)
                
            message = f"{int(per)}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed_h}:{elapsed_m}:{elapsed_s} < Remain time: {remain_h}:{remain_m}:{remain_s} **PAUSE**"
            self.statusbar.showMessage(message)
        
        # ip 차단 및 db 연결 끊김 대응
        if self.thread_crw.check == 1:
            msg = QMessageBox()
            msg.setText("\n    ** ip 차단됨 **\n\n - VPN 나라변경 필요\n - wifi 재연결 필요")
            msg.exec_()
            self.thread_crw.check = 0
        elif self.thread_crw.check == 2:
            msg = QMessageBox()
            msg.setText("\n    ** db 연결 끊김 **\n\n - VPN 연결 해제 및 wifi 재연결 필요\n\n - Upload 버튼 클릭 후 re-Run")
            msg.exec_()
            self.thread_crw.check = 0
                
    def _update_progress(self, progress):
        
        prg_dict = progress.format_dict
        itm = prg_dict['n'] 
        tot = prg_dict['total']
        per = round((itm / tot) * 100, 0)
        elapsed = int(round(prg_dict['elapsed'], 0))
        if itm >= 1:
            remain_time = int(round((elapsed * tot / itm) - elapsed, 0))
        else:
            remain_time = 0
        
        self.progressBar_2.setValue(per)
        
        message = f"{int(per)}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed}s < Remain time: {remain_time}s "
        self.statusbar.showMessage(message)
        
        if not self.thread_code.power:
            with open(self.file_path, 'rb') as f:
                product_codes = pickle.load(f)
            products = len(product_codes)
            time = round(products * 30 / 3600, 2)
            self.Products.display(products)
            self.Time.display(time)
        
    def categ_toggled(self):
        categs = []
        
        if self.skincare.isChecked():
            categ = "스킨케어"
            categs.append(categ)
            
        if self.bodycare.isChecked():
            categ = "배쓰&바디"
            categs.append(categ)
            
        if self.makeup.isChecked():
            categ_ = ["페이스메이크업", "아이메이크업", "립메이크업"]
            for categ in categ_:
                categs.append(categ)
            
        if self.haircare.isChecked():
            categ = "헤어"
            categs.append(categ)
            
        if self.cleansing.isChecked():
            categ = "클렌징"
            categs.append(categ)
            
        if self.menscare.isChecked():
            categ = "남성화장품"
            categs.append(categ)
            
        if self.suncare.isChecked():
            categ = "선케어"
            categs.append(categ)
            
        if self.maskpack.isChecked():
            categ = "마스크/팩"
            categs.append(categ)
            
        if self.beauty_tool.isChecked():
            categ = "뷰티툴"
            categs.append(categ)
            
        if self.fragrance.isChecked():
            categ = "프래그런스"
            categs.append(categ)
            
        return categs
    
    def _select(self):
        selections = self.categ_toggled()
        if len(selections) == 0:
            msg = QMessageBox()
            msg.setText("** 한개 이상의 카테고리를 선택하세요 **")
            msg.exec_()
        
        else:            
            if self.checkBox.isChecked():
                df_mapped = self.thread_crw._get_tbl()
                while len(df_mapped) == 0:
                    msg = QMessageBox()
                    msg.setText("\n    ** db 연결 끊김 **\n\n - VPN 연결 해제 및 wifi 재연결 필요\n\n")
                    msg.exec_()
                    df_mapped = self.thread_crw._get_tbl()
                    
                df_mapped_categ = df_mapped.loc[df_mapped.selection.isin(selections)]
                product_codes = df_mapped_categ.product_code.unique().tolist()
                with open(self.file_path, 'wb') as f:
                    pickle.dump(product_codes, f)
                    
            else:            
                with open(self.selections, 'wb') as f:
                    pickle.dump(selections, f)
                    
                while True:
                    try:
                        gl = self.db.get_tbl('glowpick_product_info_final_version', ['selection', 'division'])
                        break
                    except:
                        msg = QMessageBox()
                        msg.setText("\n    ** db 연결 끊김 **\n\n - VPN 연결 해제 및 wifi 재연결 필요\n\n")
                        msg.exec_()
                
                divisions = []
                for sel in selections:
                    div = list(set(gl.loc[gl.selection==sel, 'division'].values.tolist()))
                    divisions += div
                with open(self.divisions, 'wb') as f:
                    pickle.dump(divisions, f)
                    
            msg = QMessageBox()
            msg.setText("Selection done!")
            msg.exec_()
            
    def _run_prd_codes(self):
        ''' Run crawling product codes thread '''
        if not self.thread_code.power:
            if os.path.isfile(self.selections):            
                msg = QMessageBox()
                msg.setText("- 인터넷 연결 확인 \n- VPN 연결 확인 \n- mac 자동 잠금 해제 확인")
                msg.exec_()
                
                # get category index
                self.thread_code.find_category_index()
                
                # start thread
                self.thread_code.power = True
                self.thread_code.start()
            else:
                msg = QMessageBox()
                msg.setText("** Select 완료 후 시도하세요 **")
                msg.exec_()
        else:
            pass
        
    def _run_crawling(self):
        ''' Run crawling products thread '''
        if not self.thread_crw.power:
            if os.path.isfile(self.file_path):
                msg = QMessageBox()
                msg.setText("- 인터넷 연결 확인 \n- VPN 연결 확인 \n- mac 자동 잠금 해제 확인")
                msg.exec_()
                self.thread_crw.power = True
                self.thread_crw.start()
            else:
                msg = QMessageBox()
                msg.setText("** 신규 상품코드 수집 완료 후 시도하세요 **")
                msg.exec_()    
        else:
            pass
        
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
            self.viewer._loadFile('gl_info.csv')
        else:
            msg = QMessageBox()
            msg.setText('일시정지 후 다시 시도해주세요')
            msg.exec_()