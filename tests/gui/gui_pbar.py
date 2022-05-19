import os
import sys
import time
import pickle
import pandas as pd
from tqdm.auto import tqdm
# from gui import main

cur_dir = os.path.dirname(os.path.realpath(__file__))
root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
src = os.path.abspath(os.path.join(cur_dir, os.pardir))
tbl_cache = root + '/tbl_cache'
sys.path.append(root)
sys.path.append(src)
sys.path.append(tbl_cache)

print('\n\n',  cur_dir, '\n', root, '\n\n')

# from access_database import access_db
# from mapping import preprocessing
# from mapping import mapping_product


from PyQt5 import uic
from PyQt5 import QtCore
from PyQt5.QtWidgets import *

pbar_form = uic.loadUiType(cur_dir + '/form/pbar_.ui')[0]

class PbarForm(QMainWindow, pbar_form):
    
    def __init__(self):
        super().__init__()    
        self.setupUi(self)
        self.thread = ThreadClass()
        self.thread.progress.connect(self.update_progress)
        self.start.clicked.connect(self.thread.start)
        
    def update_progress(self, progress):
        
        prg_dict = progress.format_dict
        itm = prg_dict['n'] + 1
        tot = prg_dict['total']
        per = round((itm / tot) * 100, 0)
        elapsed = round(prg_dict['elapsed'], 0)
        remain_time = round((elapsed * tot / itm) - elapsed, 0)
        
        self.pbar.setValue(per)
        
        message = f"{int(per)}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed}s < Remain time: {remain_time}s "
        self.statusbar.showMessage(message)
        
            
            
class ThreadClass(QtCore.QThread, QtCore.QObject):
    progress = QtCore.pyqtSignal(object)
    
    def __init__(self):
        super().__init__()
        
    def run(self):
        t = tqdm(range(79))
        for e in t:
            self.progress.emit(t)
            time.sleep(0.1)
            
    def stop(self):
        self.terminate()
        
        
if __name__ == '__main__':
    app = QApplication(sys.argv)
    form = PbarForm()
    form.show()
    sys.exit(app.exec_())