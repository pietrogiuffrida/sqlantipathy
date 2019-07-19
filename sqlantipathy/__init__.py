#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pyodbc
import logging

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


def make_connection_string():
    return ""


class SqlAntipathy:

    insert_statement = """INSERT INTO {0} ({1}) VALUES ({2})"""

    def __init__(self):
        self.connection_string = make_connection_string()
        self.connection = self.open_connection()
        self.cursor = self.open_cursor()

    def open_connection(self):
        """Apre la connessione al server MSSQL

        Raises:
            ConnectionError: Restituisce errore se la connessione fallisce

        Returns:
            str: Accesso al Server
        """
        try:
            logger.debug("Tring to connect")
            connection = pyodbc.connect(self.connection_string, timeout=self.timeout)
        except:
            logger.error("COULD NOT PERFORM CONNECTION TO DB")
            logger.exception("")
            raise ConnectionError("Connection failed!")
        return connection

    def open_cursor(self):
        """Accesso al cursore del server MSSQL"""
        logger.debug("Opening cursor")
        return self.connection.cursor()

    def use_database(self, dbname):
        """Permette l'accesso a un database all'interno del server MSSQL

        Args:
            dbname (str): Nome del database a cui si desidera accedere
        """
        self.cursor.execute("USE {}".format(dbname))

    def retrieve(self):
        pass

    def insert_one(self):
        pass

    def insert_many(self):
        pass

    def bulk_insertion(self):
        pass

    def commit(self):
        pass

    def close_connection(self):
        """Chiude la connessione al server MSSQL"""
        logger.debug("Closing connection")
        self.connection.close()
