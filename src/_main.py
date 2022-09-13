import os
import sys
import warnings
from gui.gui_main import MainWidget
from PyQt5.QtWidgets import QApplication

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("default") # Change the filter in this process
    os.environ["PYTHONWARNINGS"] = "default" # Also affect subprocesses

# pyinstaller에 의한 패키징이 여부에 따른 경로 설정 
if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
    
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir))

tbl_cache_dir = os.path.join(root, 'tbl_cache')

# 테이블 캐시 데이터 저장 디렉토리
if not os.path.exists(tbl_cache_dir):
    os.makedirs(tbl_cache_dir)
    
# base_path
print(f'\n\nroot: {root}\n\n')

''' main gui execution '''
app = QApplication(sys.argv)
form = MainWidget()
form.show()
sys.exit(app.exec_())