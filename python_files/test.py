import sys
sys.path.insert(0, r'C:\\User\medium\pysqlplus\lib')

import os
import pandas as pd

from data import Sql

sql = Sql('val1')

directory = r'C:\Users\admin\Desktop\Weather_parser\data_files\normals-annualseasonal\\'  # место хранения сгенерированных данных

file_list = os.listdir(directory)  # определить список всех файлов

for file in file_list:
    df = pd.read_csv(directory+file)
    sql.push_dataframe(df, file[:-4])
    
# конвертируем список имен из file_list в имена таблиц
table_names = [x[:-4] for x in file_list]

sql.union(table_names, 'generic_jan')  # объединяем файлы в одну таблицу
sql.drop(table_names)

# определяем список категорий в colX, например ['hr', 'finance', 'tech', 'c_suite']
sets = list(sql.manual("SELECT colX AS 'category' FROM generic_jan GROUP BY colX", response=True)['category'])

for category in sets:
    sql.manual("SELECT * INTO generic_jan_"+category+" FROM generic_jan WHERE colX = '"+category+"'")