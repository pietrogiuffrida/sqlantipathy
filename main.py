#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from sqlantipathy import MssqlAntipathy
import pandas as pd

if __name__ == '__main__':

    sql = MssqlAntipathy(
        hostname="sql_hostname",
        user="sql_user",
        pwd="sql_pwd",
        driver="sql_driver_name"
    )

    sql.connect()

    database_list = sql.show_databases()

    sql.use_database("mydb")
    mydb_tables = sql.show_tables()

    qry = """SELECT TOP 100 * FROM TABLENAME"""
    data = sql.retrieve("sql_input_db", qry)

    list_of_dict = sql.retrieve("sql_input_db", qry)
    df = pd.DataFrame(list_of_dict)

    sql.close_connection()

    # A lot of code after...

    sql.connect()
    sql.cursor.execute("""A SIMPLE QUERY""")
    raw_data = sql.cursor.fetchall()