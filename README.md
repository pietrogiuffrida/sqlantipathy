# sqlantipathy
Python facilities to easily work with SQL databases

Ok, I don't love work with SQL databases. But the world works with SQL, then...

During last years I wrote lot of function to work with MSSQL, MySQL, Oracle, SQLite...<br>
This project represent my personal attempt to systematize experiences, code,
and approaches in few useful classes.

Of course sqlalchemy is a sort of *de facto* standard in python/SQL approach,
and my package will never be such mature... but in my opinion it is not so simple and not ever
backward compatibility is guaranteed with pyodbc and other low level libraries.

At this moment master branch only implements MSSQL routines.
MySQL and Oracle rootines will be added as soon as possible.

# Installation

Install sqlantipathy is as easy as run
```pip install sqlantipathy```.

# Usage

A more accurate description of methods included in
sqlantipathy will follow. By now, you can refers
to main.py file content:

```python
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
```