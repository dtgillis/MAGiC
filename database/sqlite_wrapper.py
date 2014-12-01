__author__ = 'dtgillis'

import sqlite3


class SqliteConnector():
    """
    Easy class for connecting to a database

    """

    def __init__(self, db_name='tmp', write_privelage=False):

        self.db_name = db_name
        self.write_access = write_privelage
        self.connection = None
        self.connected = False

    def connect_to_database(self):

        if self.connected:
            print "Already connected to database {0:s}".format(self.db_name)

        else:
            try:
                self.connection = sqlite3.connect(self.db_name)
                self.connected = True
            except Exception as e:
                print 'Trouble opening database {0:s}'.format(self.db_name)

    def close_connection_to_database(self):

        if not self.connected:
            print "Cannot disconect from a database you are not connected to"
        else:
            try:
                self.connection.commit()
                self.connection.close()
            except Exception as e:
                print e.message
                print "trouble disconnecting from database"

    def get_cursor(self):

        if not self.connected:
            self.connect_to_database()

        return self.connection.cursor()

    def execute_many_statements(self, statements):
        """
        :param statements: Should be a list of tuples like ("SQL STATEMENT ???",(values))
        :return: no return
        """
        if not self.connected:
            self.connect_to_database()
        try:
            if len(statements) > 0:
                self.connection.executemany()
            else:
                print "No statements for writing to the db"
        except Exception as e:

            print e.message
            self.connection.rollback()
        finally:
            self.connection.commit()


    def execute_single_write(self, statement):
        """
        :param statement: Should be a list of tuples like ("SQL STATEMENT ???",(values))
        :return: no return
        """
        if not self.connected:
            self.connect_to_database()
        try:
            if len(statement) > 0:
                self.connection.executemany()
            else:
                print "No statements for writing to the db"
        except Exception as e:
            print e.message
            self.connection.rollback()
        finally:
            self.connection.commit()

    def execute_sql_script(self, script):
        """
        :param script: Will try and execute a script given by the user
        :return:
        """

        if not self.connected:
            self.connect_to_database()
        else:
            try:
                self.connection.executescript(script)
            except Exception as e:
                print e.message
                self.connection.rollback()
            finally:
                self.connection.commit()


class SqliteWriter():

    def __init__(self, db_connection):

        assert isinstance(db_connection, SqliteConnector)
        self.db_connection = db_connection
        self.cursor = self.db_connection.get_cursor()

    def execute_statement(self, statement, multiple_statements=False):

        if not self.db_connection.connected:
            self.db_connection.connect_to_database()
        if multiple_statements:
            self.db_connection.execute_many_statements(statement)
        else:
            self.db_connection.execute_single_write(statement)

