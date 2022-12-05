import os
import user_agent
ua_dir = os.path.abspath(os.path.join(user_agent.__file__, os.pardir))
ua_data = os.path.join(ua_dir, 'data')

add_module = [
    './src/access_database/.py',
    './src/hangle/.py',
    './src/mapping/.py',
    './src/scraping/.py',
    './src/gui/.py',
    './src/multithreading/.py',
    './src/reviews/.py',
]

add_file = [
    './src/gui/form/*.ui:form',
    f'{ua_data}/*.json:user_agent/data',
]

exclude_module = [
    './.ipynb',
]

icon = './src/mycelebs_CI.icns'

name = "DataManager"

_py = "./src/_main.py"

# init command
command = "pyinstaller -F" # onefile
# command = "pyinstaller"

# add module
for md in add_module:
    _md = f' --path "{md}"'
    command += _md
    
# add file
for f in add_file:
    _f = f' --add-data "{f}"'
    command += _f

# # set icon
# command += f' --i={icon}'

# # exclude module
# for md in exclude_module:
#     _md = f' --exclude "{md}"'
#     command += _md

# name
command += f' --name "{name}"'
# .py
command += f' {_py}'

if __name__ == "__main__":
    os.system(command)