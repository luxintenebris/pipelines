import sqlite3
import csv
import pandas as pd
import sqlite3

class BaseTask:
    """Base Pipeline Task"""

    def run(self):
        raise RuntimeError('Do not run BaseTask!')

    def short_description(self):
        pass

    def __str__(self):
        task_type = self.__class__.__name__
        return f'{task_type}: {self.short_description()}'


class CopyToFile(BaseTask):
    """Copy table data to CSV file"""

    def __init__(self, table, output_file):
        self.table = table
        self.output_file = output_file

    def short_description(self):
        return f'{self.table} -> {self.output_file}'

    def run(self):
        sqlite_connection = sqlite3.connect("task.db")
        cursor = sqlite_connection.cursor()

        # data = [('id', 'name', 'url', 'domain_of_url')]
        # for row in cursor.execute("SELECT * FROM " + self.table):
        #     data.append(row)
            
        # myFile = open(self.output_file + '.csv', 'w', newline='')
        # with myFile:
        #     writer = csv.writer(myFile)
        #     writer.writerows(data)

        clients = pd.read_sql('SELECT * FROM '+ self.table, sqlite_connection)
        clients.to_csv(self.output_file+".csv", index=False)


        print(f"Copy table `{self.table}` to file `{self.output_file}`")


class LoadFile(BaseTask):
    """Load file to table"""

    def __init__(self, table, input_file):
        self.table = table
        self.input_file = input_file

    def short_description(self):
        return f'{self.input_file} -> {self.table}'

    def run(self):
        df = pd.read_csv(self.input_file)
        sqlite_connection = sqlite3.connect("task.db")
        df.to_sql(self.table, sqlite_connection, if_exists='append', index=False)
        sqlite_connection.commit()

        print(f"Load file `{self.input_file}` to table `{self.table}`")


class RunSQL(BaseTask):
    """Run custom SQL query"""

    def __init__(self, sql_query, title=None):
        self.title = title
        self.sql_query = sql_query

    def short_description(self):
        return f'{self.title}'

    def run(self):
        sqlite_connection = sqlite3.connect('task.db')
        cursor = sqlite_connection.cursor()
        cursor.execute(self.sql_query)
        sqlite_connection.commit()
        cursor.close()
        print(f"Run SQL ({self.title}):\n{self.sql_query}")



class CTAS(BaseTask):
    """SQL Create Table As Task"""

    def __init__(self, table, sql_query, title=None):
        self.table = table
        self.sql_query = sql_query
        self.title = title or table

    def short_description(self):
        return f'{self.title}'

    def run(self):
        sqlite_connection = sqlite3.connect("task.db")
        cursor = sqlite_connection.cursor()

        def _domain_of_url(x):
            res = str(x).partition("://")[2].partition("/")[0]
            print(res)
            return res

        sqlite_connection.create_function("domain_of_url", 1, _domain_of_url)
        cursor.execute("Create table " + self.table + " as " + self.sql_query)

        print(f"Create table `{self.table}` as SELECT:\n{self.sql_query}")
