import psycopg2
from contextlib import closing
from psycopg2.extras import execute_values
import pandas as pd
from psycopg2.extras import RealDictCursor
from os import getenv


class DBPostgresql:
    def __init__(self, db_dwh_conn):
        self.__dbname = db_dwh_conn['schema']
        self.__user = db_dwh_conn['login']
        self.__password = db_dwh_conn['pass']
        self.__host = db_dwh_conn['host']
        self.__port = int(db_dwh_conn['port'])

    def select(self, sql):
        try:
            with closing(psycopg2.connect(dbname=self.__dbname,
                                          user=self.__user,
                                          password=self.__password,
                                          host=self.__host,
                                          port=self.__port)) as conn:
                with conn.cursor() as cursor:
                    conn.autocommit = True
                    cursor.execute(sql)
                    return cursor.fetchall()
        except psycopg2.Error:
            raise psycopg2.Error

    def select_dataframe(self, sql, columns):
        try:
            with closing(psycopg2.connect(dbname=self.__dbname,
                                          user=self.__user,
                                          password=self.__password,
                                          host=self.__host,
                                          port=self.__port)) as conn:
                with conn.cursor() as cursor:
                    conn.autocommit = True
                    cursor.execute(sql)
                    df = pd.DataFrame(cursor.fetchall(), columns=columns)
                    df.convert_dtypes()
                    return df
        except psycopg2.Error:
            raise psycopg2.Error

    def insert(self, sql):
        try:
            with closing(psycopg2.connect(dbname=self.__dbname,
                                          user=self.__user,
                                          password=self.__password,
                                          host=self.__host,
                                          port=self.__port)) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    conn.commit()
        except psycopg2.Error:
            raise psycopg2.Error

    def insert_many(self, sql, data):
        try:
            with closing(psycopg2.connect(dbname=self.__dbname,
                                          user=self.__user,
                                          password=self.__password,
                                          host=self.__host,
                                          port=self.__port)) as conn:
                with conn.cursor() as cursor:
                    execute_values(cursor, sql, data)
                    conn.commit()
        except psycopg2.Error:
            raise psycopg2.Error

    def select_as_dict(self, sql):
        try:
            with closing(psycopg2.connect(dbname=self.__dbname,
                                          user=self.__user,
                                          password=self.__password,
                                          host=self.__host,
                                          port=self.__port)) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                    conn.autocommit = True
                    cursor.execute(sql)
                    return [dict(row) for row in cursor.fetchall()]
        except psycopg2.Error:
            raise psycopg2.Error

