#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import numpy as np
import logging

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


class SqlBasic:

    connection_string_schema = "{user} {password} {hostname}"

    def __init__(
            self,
            hostname,
            user,
            password,
            timeout,
    ):
        self.hostname = hostname
        self.user = user
        self.password = password
        self.timeout = timeout

        self.connection = None
        self.cursor = None

    def connect(self):
        self.make_connection_string()
        self.open_connection()
        self.open_cursor()

    def make_connection_string(self):
        """Format a valid connection string for each db

        This method *CAN be re-writed* for each database engine if is not sufficient
        overwrite self.connection_string property


        Returns:
            a valid connection string
        """
        self.connection_string = self.connection_string_schema.format(
            hostname=self.hostname,
            user=self.user,
            password=self.password,
        )

    def open_connection(self):
        """Open and returns a connection to the db.

        This method must be re-writed for each database engine.

        Returns:
            a connection object

        Examples:
            ```connection = pyodbc.connect(self.connection_string, timeout=self.timeout)```
        """
        print(self.connection_string)
        self.connection = None

    def open_cursor(self):
        logger.debug("Opening cursor")
        self.cursor = self.connection.cursor()

    def commit(self):
        self.cursor.commit()

    def close_connection(self):
        logger.debug("Closing connection")
        self.connection.close()


class SqlAntipathy(SqlBasic):

    show_tables_query = "SHOW TABLES"
    show_databases_query = "SHOW DATABASES"
    use_database_statement = "USE {}"
    insert_statement = """INSERT INTO {0} ({1}) VALUES ({2})"""

    def __init__(self, hostname, user, password, timeout, connect=False):

        super().__init__(hostname, user, password, timeout)
        self.hostname = hostname
        self.user = user
        self.password = password
        self.timeout = timeout

    def use_database(self, dbname):
        self.cursor.execute(
            self.use_database_statement.format(dbname)
        )

    def show_databases(self):
        self.cursor.execute(self.show_databases_query)
        databases = self.cursor.fetchall()
        return [i[0] for i in databases]

    def show_tables(self, dbname):
        self.use_database(dbname)
        self.cursor.execute(self.show_tables_query)
        tables = self.cursor.fetchall()
        return [i[0] for i in tables]

    def retrieve(self, dbname, qry):
        """Run a query and collect all results

        Args:
            dbname (str): the name of the query
            qry (str): a query string

        Returns:
            list: a list of tuple, the results of the query
        """

        self.use_database(dbname=dbname)

        logger.debug("Running query")
        self.cursor.execute(qry)

        logger.debug("Reading data")
        return self.cursor.fetchall()

    def retrieve_table(self, dbname, qry):
        """Run the query and returns a list of dict

        Each record are represented as a dict, to easily transform data
        in a pandas dataframe

        Args:
            dbname:
            qry:

        Returns:
            list of dict

        """
        logger.debug("Parsing data")
        values = self.retrieve(dbname=dbname, qry=qry)
        keys = [i[0] for i in self.cursor.description]

        data = []
        for record in values:
            parsed = {}
            for key, value in zip(keys, record):
                parsed[key] = value
            data.append(parsed)

        logger.debug("Data parsed")

        return data

    def insert_one(self, table_name, values, dbname=None):
        logger.debug("insert_one {0}.{1}".format(dbname, table_name))

        if dbname:
            self.use_database(dbname)

        statement = ""
        try:
            fields, values = self.make_list_of_values(values)
            statement = self.insert_statement.format(table_name, fields, values)
            self.cursor.execute(statement)
        except:
            logger.error("insert_one statement: {0}".format(statement or None))
            logger.exception("")
            return 1
        return 0

    def insert_many(self, data, dbname, table_name, step=5000):
        logger.debug("insert_many {0}.{1}".format(dbname, table_name))

        self.use_database(dbname)

        len_data = len(data)
        errors = 0

        try:
            for idx, row in enumerate(data):

                if idx > 0 and idx % step == 0:
                    logger.info("Arrivato a {0}/{1}".format(idx, len_data))
                    self.connection.commit()

                list_of_columns, list_of_values = self.make_list_of_values(row)

                statement = "NONE"
                try:
                    statement = self.insert_statement.format(
                        table_name, list_of_columns, list_of_values
                    )
                    self.cursor.execute(statement)
                except:
                    logger.error("Errore a idx {0}".format(idx))
                    logger.error("Statement {}".format(statement))
                    logger.exception("")
                    errors += 1
                    if errors > 5:
                        return 1
                    continue

            self.connection.commit()
            return 0
        except:
            logger.error("CARICAMENTO DATI FALLITO!!!")
            logger.error("Last row ({0}) {1}".format(idx, row or None))
            logger.exception("")
            return 1

    def bulk_insertion(self):
        """Perform a bulk insertion.

        This method must be re-writed for each db engine."""
        pass

    def make_list_of_values(self, values_dict, list_of_columns=None, missing_value=None):
        if not list_of_columns:
            list_of_columns = values_dict.keys()

        values = ""
        columns = ""
        for key in list_of_columns:
            values += self.sql_clean(values_dict.get(key, missing_value))
            columns += ", "
            columns += key

        values = values.strip(",")
        columns = columns.strip(",")

        return columns, values

    def sql_clean(self, value):
        if type(value) == float and np.isnan(value):
            return "NULL,"
        if value in ["", "NULL", None]:
            return "NULL,"
        if type(value) == str:
            value = re.sub(r'"*|:?\\+|/', "", value).strip()
            value = value.replace("'", "''")
        return "'{}',".format(value)
