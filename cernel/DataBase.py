import mysql.connector


class MySQL:
    def __init__(self, host, user, passwd):
        try:
            self.MYSQL = mysql.connector.connect(
                host=host,
                user=user,
                passwd=passwd
            )
        except mysql.connector.Error:
            print('failed to connect to MySQL, check the entered parameters')

    def showDatabases(self):
        my_cursor = self.MYSQL.cursor()
        my_cursor.execute(f'SHOW DATABASES')
        mass = []
        for x in my_cursor:
            for y in x:
                mass.append(y)
        mass.sort()
        my_cursor.close()
        return mass

    def userDatabases(self):
        all_bases_list = self.showDatabases()
        system_bases_list = ['information_schema', 'mysql', 'performance_schema', 'sys']
        user_bases = list(set(all_bases_list) - set(system_bases_list))
        user_bases.sort()
        return user_bases

    @staticmethod
    def systemDatabases():
        system_bases_list = ['information_schema', 'mysql', 'performance_schema', 'sys']
        return system_bases_list


class DataBase:
    # create a database and connect to it
    def __init__(self, host, user, passwd, database):
        try:
            self.base = mysql.connector.connect(
                host=host,
                user=user,
                passwd=passwd,
            )
            my_cursor = self.base.cursor()
            my_cursor.execute(f'CREATE DATABASE {database}')
        except mysql.connector.Error:
            if mysql.connector.errors:
                mysql.connector.errors.Error()
            else:
                pass
        self.ConnectDataBase(host, user, passwd, database)

    # connect to database
    def ConnectDataBase(self, host, user, passwd, database):
        try:
            self.base = mysql.connector.connect(
                host=host,
                user=user,
                passwd=passwd,
                database=database
            )
        except mysql.connector.Error:
            print(f'database {database} not found')

    def DropDatabase(self, drop_list):
        for i in drop_list:
            try:
                my_cursor = self.base.cursor()
                my_cursor.execute(f'DROP DATABASE {i}')
            except mysql.connector.Error:
                return 'database unavailable'

    def DropTable(self, drop_list):
        for i in drop_list:
            try:
                my_cursor = self.base.cursor()
                my_cursor.execute(f'DROP TABLE {i}')
                my_cursor.close()
            except mysql.connector.Error:
                return 'table unavailable'

    def DropColumn(self, table_name, column_list):
        for i in column_list:
            try:
                my_cursor = self.base.cursor()
                my_cursor.execute(f'ALTER TABLE {table_name} DROP COLUMN {i}')
                my_cursor.close()
            except mysql.connector.Error:
                return 'cannot delete column'

    def DropRow(self, table_name, column_name, what_to_delete):
        my_cursor = self.base.cursor()
        sql = f'DELETE FROM {table_name} WHERE {column_name} = \'{what_to_delete}\''
        # sql = "DELETE FROM " + table_name + " WHERE " + column_name + " =" + "\'" + what_to_delete + "\'"
        my_cursor.execute(sql)
        self.base.commit()
        my_cursor.close()

    def Truncate(self, table_name):
        try:
            my_cursor = self.base.cursor()
            sql = f"TRUNCATE {table_name}"
            my_cursor.execute(sql)
            my_cursor.close()
        except mysql.connector.Error:
            mysql.connector.Error()

    def CreateTable(self, list_tables_create):
        for i in list_tables_create:
            try:
                my_cursor = self.base.cursor()
                my_cursor.execute(f'CREATE TABLE {i} (id INT AUTO_INCREMENT PRIMARY KEY)')
                my_cursor.close()
            except mysql.connector.Error:
                return 'unable to create table'

    def CreateTableWeights(self, table_name, columns):
        try:
            my_cursor = self.base.cursor()
            my_cursor.execute(f'CREATE TABLE {table_name} ({columns})')
            my_cursor.close()
        except mysql.connector.Error:
            print('unable to create table')

    def AddColumn(self, table_name, columns):
        try:
            for column_name, data_type in columns.items():
                my_cursor = self.base.cursor()
                my_cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN {column_name} {data_type}')
                my_cursor.close()
        except mysql.connector.Error:
            return 'unable add column'

    def SelectColumn(self, table_name, column_name):
        try:
            mass = []
            my_cursor = self.base.cursor()
            my_cursor.execute(f'SELECT {column_name} FROM {table_name}')
            my_result = my_cursor.fetchall()
            for x in my_result:
                for y in x:
                    mass.append(y)
            my_cursor.close()
            return mass
        except mysql.connector.Error:
            return 'cannot select data'

    def SelectRows(self, table_name):
        try:
            my_cursor = self.base.cursor()
            my_cursor.execute(f'SELECT * FROM {table_name}')
            my_result = my_cursor.fetchall()
            val = []
            for i in my_result:
                i = list(i)
                val.append(i)
            my_cursor.close()
            my_cursor.close()
            return val
        except mysql.connector.Error:
            return 'cannot select data'

    def SelectRowId(self, table_name, id):
        try:
            my_cursor = self.base.cursor()
            my_cursor.execute(f'SELECT * FROM {table_name} WHERE id={id}')
            my_result = my_cursor.fetchall()
            val = []
            for i in my_result:
                i = list(i)
                val.append(i)
            my_cursor.close()
            return val
        except mysql.connector.Error:
            return 'cannot select data'

    def SelectCountRows(self, table_name):
        try:
            my_cursor = self.base.cursor()
            my_cursor.execute(f'SELECT COUNT(1) FROM {table_name}')
            my_result = my_cursor.fetchall()
            result = my_result[0]
            my_cursor.close()
            return int(result[0])
        except mysql.connector.Error:
            return 'cannot select data'

    def InsertInto(self, table_name, lists_values_in_list, list_column_info):
        try:
            values_tuples = []
            for i in lists_values_in_list:
                i = tuple(i)
                values_tuples.append(i)
            if 'id' in list_column_info:
                list_column_info.remove('id')
            mass = []
            for i in range(len(list_column_info)):
                mass.append('%s')
            mass = self.ListToString(mass)
            line = mass.split()
            values = ', '.join(line)
            string_columns = self.GetStringForRequest(list_column_info)
            my_cursor = self.base.cursor()
            for i in values_tuples:
                sql = f'INSERT INTO {table_name} ({string_columns}) VALUES ({values})'
                my_cursor.execute(sql, i)
                self.base.commit()
            my_cursor.close()
        except mysql.connector.Error:
            return mysql.connector.ProgrammingError()

    def Search(self, table_name, column_name, text_search):
        my_cursor = self.base.cursor()
        sql = f'SELECT * FROM {table_name} WHERE {column_name} = \'{text_search}\''
        my_cursor.execute(sql)
        result = my_cursor.fetchall()
        my_cursor.close()
        return result

    def SearchLike(self, table_name, column_name, expression_for_search):
        my_cursor = self.base.cursor()
        sql = f'SELECT * FROM {table_name} WHERE {column_name} LIKE \'{expression_for_search}\''
        my_cursor.execute(sql)
        result = my_cursor.fetchall()
        my_cursor.close()
        return result

    def Update(self, table_name, column_name, old_data, new_data):
        my_cursor = self.base.cursor()
        sql = f'UPDATE {table_name} SET {column_name} = \'{new_data}\' WHERE {column_name} = \'{old_data}\''
        my_cursor.execute(sql)
        my_cursor.close()
        self.base.commit()

    def UpdateDataById(self, table_name, column_list: list, data_list: list, id: int):
        my_cursor = self.base.cursor()
        query_list = []
        for i in range(len(column_list)):
            if type(data_list[i]) == str:
                data_list[i] = f'"{data_list[i]}"'
            query_list.append(f'{column_list[i]}={data_list[i]},')
        if ',' in query_list[len(query_list) - 1]:
            query_list[len(query_list) - 1] = (query_list[len(query_list) - 1]).replace(",", "")
        sql = f'UPDATE {table_name} SET {" ".join(query_list)} WHERE id={id}'
        my_cursor.execute(sql)
        self.base.commit()
        my_cursor.close()

    def ShowDataTable(self, table_name):
        my_cursor = self.base.cursor()
        my_cursor.execute(f'SELECT * FROM {table_name}')
        result = my_cursor.fetchall()
        my_cursor.close()
        return result

    def ShowTablesList(self):
        tables = []
        my_cursor = self.base.cursor()
        my_cursor.execute(f'SHOW TABLES')
        for x in my_cursor:
            for y in x:
                tables.append(y)
        my_cursor.close()
        return tables

    def ShowColumnInfo(self, database, table_name):
        my_cursor = self.base.cursor()
        my_cursor.execute(f'SHOW COLUMNS FROM {table_name} IN {database}')
        mass = []
        for x in my_cursor:
            mass.append(x)
        return mass

    def GetColumnList(self, database, table_name):
        column_info = self.ShowColumnInfo(database, table_name)
        column_list = self.GetColumnInfo(column_info, 0)
        return column_list

    def GetLastId(self, table_name):
        my_cursor = self.base.cursor()
        my_cursor.execute(f'SELECT MAX(id) FROM {table_name}')
        my_result = my_cursor.fetchall()
        result = my_result[0]
        my_cursor.close()
        return int(result[0])

    # Special functions
    @staticmethod
    def WriteDataToDatabase(database, dataset, table_name):
        column_info = database.ShowColumnInfo(database.base.database, table_name)
        list_column_info = database.GetColumnInfo(column_info, 0)
        database.InsertInto(table_name, dataset, list_column_info)

    @staticmethod
    def CreateTableWithColumns(database, column_names, data_types, table_name):
        database.CreateTable([table_name])
        columns = dict(zip(column_names, data_types))
        database.AddColumn(table_name, columns)

    @staticmethod
    def GetColumnInfo(list_column_info, num):
        mass = []
        if type(num) == int:
            for x in list_column_info:
                mass.append(x[num])
        return mass

    @staticmethod
    def ListToString(list_to_string):
        string = ' '
        return string.join(list_to_string)

    def GetStringForRequest(self, list_for_string):
        s = self.ListToString(list_for_string)
        line = s.split()
        string = ', '.join(line)
        return string

    @staticmethod
    def Console(result):
        for x in result:
            for j in x:
                print(j)

    @staticmethod
    def Count(result):
        count = len(result)
        return count