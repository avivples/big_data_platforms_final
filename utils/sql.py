import sqlite3 as sql

import warnings
warnings.filterwarnings('ignore')


def create_connection(db_file):
    """
    Create an sql connection to the db by the given db file

    Parameters:
    db_file (str): Path of the db file to use to make a connection to the db

    Returns:
    conn (sql.Connection): Connection to db
    """
    conn = sql.connect(db_file)
    return conn


def create_table(conn, create_table_sql):
    """
    Create a table in the db with the given sql execution code

    Parameters:
    conn (sql.Connection): connection to db
    create_table_sql (str): sql execution code for table creation
    """
    c = conn.cursor()
    c.execute(create_table_sql)
    conn.commit()
    c.close()
