import pyodbc
from datetime import datetime

class Sql:
    def __init__(self, database, server="SWORDFISH"):
        self.cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                   "Server="+server+";"
                                   "Database="+database+";"
                                   "Trusted_Connection=yes;")
        self.query = "-- {}\n\n-- Made in Python".format(datetime.now()
                                                         .strftime("%d/%m/%Y"))
        print(123)

    def push_dataframe(self, data, table="raw_data", batchsize=500):
        cursor = self.cnxn.cursor()      # создаем курсор
        cursor.fast_executemany = True   # активируем быстрое выполнение

        # создаём заготовку для создания таблицы (начало)
        query = "CREATE TABLE [" + table + "] (\n"

        # итерируемся по столбцам
        for i in range(len(list(data))):
            query += "\t[{}] varchar(255)".format(list(data)[i])  # add column (everything is varchar for now)
            # добавляем корректное завершение
            if i != len(list(data))-1:
                query += ",\n"
            else:
                query += "\n);"

        cursor.execute(query)  # запуск создания таблицы
        self.cnxn.commit()     # коммит для изменений

        # append query to our SQL code logger
        self.query += ("\n\n-- create table\n" + query)

        # вставляем данные в батчи
        query = ("INSERT INTO [{}] ({})\n".format(table,
                                                '['+'], ['  # берем столбцы
                                                .join(list(data)) + ']') +
                "VALUES\n(?{})".format(", ?"*(len(list(data))-1)))

        # вставляем данные в целевую таблицу
        for i in range(0, len(data), batchsize):
            if i+batchsize > len(data):
                batch = data[i: len(data)].values.tolist()
            else:
                batch = data[i: i+batchsize].values.tolist()
            # запускаем вставку батча
            cursor.executemany(query, batch)
            self.cnxn.commit()




    def manual(self, query, response=False):
        cursor = self.cnxn.cursor()  # создаем курсор выполнения

        if response:
            return read_sql(query, self.cnxn)
        try:
            cursor.execute(query)  # execute
        except pyodbc.ProgrammingError as error:
            print("Warning:\n{}".format(error))

        self.cnxn.commit()
        return "Query complete."
    
    def union(self, table_list, name="union", join="UNION"):
        query = "SELECT * INTO ["+name+"] FROM (\n"
        query += f'\n{join}\n'.join(
                            [f'SELECT [{x}].* FROM [{x}]' for x in table_list]
                            )
        query += ") x"
        self.manual(query, fast=True)

    def drop(self, tables):
        if isinstance(tables, str):
            # если отдельная строка, переведем в список
            tables = [tables]

        for table in tables:
            # проверяем наличие таблицы и удаляем, если существует
            query = ("IF OBJECT_ID ('["+table+"]', 'U') IS NOT NULL "
                    "DROP TABLE ["+table+"]")
            self.manual(query)