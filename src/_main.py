import os
import sys
from gui.gui_main import MainWidget
from PyQt5.QtWidgets import QApplication


# pyinstaller에 의한 패키징이 여부에 따른 경로 설정 
if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    base_path = sys._MEIPASS
    tbl_cache_dir = os.path.join(base_path, 'tbl_cache_')
    
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir))
    base_path = os.path.dirname(os.path.realpath(__file__))
    tbl_cache_dir = os.path.join(root, 'tbl_cache')
    
print(f'\n\nroot: {base_path}\n\n')
ssl = os.path.join(base_path, 'ssl') 
if os.path.isdir(ssl):
    file = os.listdir(ssl)
    print(file, '\n\n')

# 테이블 캐시 데이터 저장 디렉토리
if not os.path.exists(tbl_cache_dir):
    os.makedirs(tbl_cache_dir)
    
def _exec_gui():
    ''' main gui execution '''
    app = QApplication(sys.argv)
    form = MainWidget()
    form.show()
    sys.exit(app.exec_())
    
''' Opne GUI '''
if __name__ == '__main__':
    _exec_gui()