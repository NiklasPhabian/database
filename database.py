import sqlite3
import configparser
import psycopg2
import psycopg2.extras
import pandas
import sqlalchemy


class Database:
    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()

    def make_engine(self):
        if self.engine is None:
            self.engine = sqlalchemy.create_engine(self.uri, pool_size=10)


class PostgresDatabase(Database):
    def __init__(self, config_file, config_name):
        self.user = None
        self.pwd = None
        self.connection = None
        self.cursor = None
        self.dict_cursor = None
        self.host = None
        self.db_name = None
        self.uri = None
        self.engine = None
        self.load_config(config_file, config_name)
        self.db_type = 'postgres'
        self.placeholder = '%s'
        self.bool_function = 'BOOL'
        self.connect()
        self.make_engine()

    def load_config(self, config_file, config_name):
        config = configparser.ConfigParser(allow_no_value=True)
        config.optionxform = str
        config.read(config_file)
        self.host = config[config_name]['host']
        self.user = config[config_name]['user']
        self.pwd = config[config_name]['pwd']
        self.db_name = config[config_name]['database']
        self.uri = 'postgresql+psycopg2://{user}:{pwd}@{host}/{db_name}'
        self.uri = self.uri.format(user=self.user, pwd=self.pwd, host=self.host, db_name=self.db_name)

    def connect(self):
        connection_str = "host='{}' dbname='{}' user='{}' password='{}'".format(self.host, self.db_name, self.user, self.pwd)
        self.connection = psycopg2.connect(connection_str)
        self.cursor = self.connection.cursor()
        self.dict_cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)


class SQLiteDatabase(Database):
    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path)
        self.cursor = self.connection.cursor()
        self.uri = 'sqlite:///{db_path}'.format(db_path=db_path)
        self.db_type = 'sqlite'
        self.placeholder = '?'
        self.bool_function = ''
        self.make_engine()


class DBTable:
    def __init__(self, database, table_name, schema=None):
        self.table_name = table_name
        self.database = database
        self.schema = schema
        self.cursor = self.database.cursor
        if self.schema is not None:
            self.full_table_name = self.schema + '.' + self.table_name
        else:
            self.full_table_name = self.table_name

    def clear(self):
        query = 'DELETE FROM {table_name}'
        query = query.format(table_name=self.full_table_name)
        self.database.cursor.execute(query)
        self.database.commit()

    def select_all(self):
        query = 'SELECT * FROM {table_name};'
        query = query.format(table_name=self.full_table_name)
        self.database.cursor.execute(query)

    def contains_value(self, column, value):
        query = 'SELECT exists (SELECT 1 FROM {table_name} WHERE {column} = {placeholder}  LIMIT 1);'
        query = query.format(table_name=self.table_name, column=column, placeholder=self.database.placeholder)
        with self.database.engine.connect() as conn:
            res = conn.execute(query, (value,)) 
            ret = res.fetchone()
        return ret[0]

    def drop(self):
        query = 'DROP TABLE IF EXISTS {table_name}'.format(table_name=self.full_table_name)
        query = query.format(table_name=self.full_table_name)
        self.database.cursor.execute(query)
        self.database.commit()

    def commit(self):
        self.database.commit()

    def from_dataframe(self, df):
        df.to_sql(name=self.full_table_name, con=self.database.engine, schema=self.schema, if_exists='replace', index=True)

    def append_dataframe(self, db):
        df.to_sql(name=self.full_table_name, con=self.database.engine, schema=self.schema, if_exists='append', index=True)

    def to_dataframe(self, query=None):
        if query is None:
            query = 'SELECT * FROM {table_name}'
        query = query.format(table_name=self.full_table_name)        
        df = pandas.read_sql(sql=query, con=self.database.engine)
        return df

    def to_list(self, query=None):
        if query is None:
            query = 'SELECT * FROM {table_name}'
        query = query.format(table_name=self.full_table_name)
        self.database.cursor.execute(query)
        ret = self.database.cursor.fetchall()
        return ret

    def add_pkey(self):
        query = 'ALTER TABLE {full_table_name} ADD CONSTRAINT {table_name}_pkey PRIMARY KEY(idx);'
        query = query.format(full_table_name=self.full_table_name, table_name=self.table_name)
        self.database.cursor.execute(query)
        self.database.commit()

    def max_idx(self):
        query = 'SELECT max(idx) FROM {table_name}'
        query = query.format(table_name=self.full_table_name)
        self.database.cursor.execute(query)
        ret = self.database.cursor.fetchone()
        return ret[0]

    def restart_sequence(self):
        max_idx = self.max_idx() + 1
        query = 'ALTER SEQUENCE {table_name}_idx_seq RESTART WITH {max_idx};'
        query = query.format(table_name=self.full_table_name, max_idx=max_idx)
        self.database.cursor.execute(query)
        self.database.commit()


