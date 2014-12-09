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
                self.connection.executemany(statements[0], statements[1])
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
                self.connection.execute(statement[0], statement[1])
            else:
                print "No statements for writing to the db"
        except Exception as e:
            print e.message
            self.connection.rollback()
        finally:
            self.connection.commit()

    def execute_single_lookup(self, statement):
        """
        :param statement: Should be a list of tuples like ("SQL STATEMENT ???",(values))
        :return: no return
        """
        if not self.connected:
            self.connect_to_database()
        try:
            if len(statement) > 0:
                results = self.connection.execute(statement[0], statement[1]).fetchall()
                if len(results) > 0:
                    #only one results
                    if len(results) == 1:
                        return results[0]
                    # multiple results
                    else:
                        return results
            else:
                print "No statements for lookup"

        except Exception as e:
            print e.message


    def execute_sql_script(self, script):
        """
        :param script: Will try and execute a script given by the user
        :return:
        """

        if not self.connected:
            self.connect_to_database()
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


class SqliteLookup():

    def __init__(self, db_connection):

        assert isinstance(db_connection, SqliteConnector)
        self.db_connection = db_connection

    #TODO convineince function should know number of result fields to return.
    def execute_lookup(self, statement):

        return self.db_connection.execute_single_lookup(statement)

    def snp_id_lookup(self, snp_name):

        snp_name_sql = 'select id from snp_name_lookup where snp_name = ?'

        snp_name_result = self.execute_lookup((snp_name_sql, [snp_name]))

        if snp_name_result is not None:
            return int(snp_name_result[0])
        else:
            return None

    def snp_info_lookup(self, snp_name):

        snp_name_sql = 'select id, chrm, bp from snp_name_lookup where snp_name = ?'

        snp_name_result = self.execute_lookup((snp_name_sql, [snp_name]))

        if snp_name_result is not None:
            return snp_name_result
        else:
            return None

    def methyl_probe_id_lookup(self, probe_name):
        #sql for methyl probe name
        methyl_probe_name_sql = 'select id from methyl_probe_name_lookup where probe_name = ?'

        methyl_probe_result = self.execute_lookup((methyl_probe_name_sql, [probe_name]))

        if methyl_probe_result is not None:
            return int(methyl_probe_result[0])
        else:
            return None

    def methyl_probe_name_lookup(self, methyl_id):

        #sql for methyl probe name
        methyl_probe_name_sql = 'select probe_name from methyl_probe_name_lookup where id = ?'

        methyl_probe_result = self.execute_lookup((methyl_probe_name_sql, [methyl_id]))

        if methyl_probe_result is not None:
            return str(methyl_probe_result[0])
        else:
            return None

    def gwas_methyl_lookup_table_search(self, sample_name, by_gwas=True):

        if by_gwas:
            sql = 'select id from gwas_methyl_lookup where gwas_sample_id = ?'
        else:
            sql = 'select id from gwas_methyl_lookup where methyl_sample_id = ?'

        sample_result = self.execute_lookup((sql, [sample_name]))
        if sample_result is not None:
            return int(sample_result[0])
        else:
            return None

    def get_distinct_gemes_snps(self):
        """
        :return: list of tuples ( snp_id, snp_name) distinct gemes snps
        """
        sql = 'select distinct g.snp_id,snl.snp_name from gemes as g join snp_name_lookup snl where snl.id=g.snp_id'

        return self.execute_lookup((sql, []))

    def get_methyl_probes_in_geme_pair_by_snp_id(self, snp_id):

        sql = 'select probe_id from gemes where snp_id=?'

        results = self.execute_lookup((sql, [snp_id]))

        return_list = []

        if results is None:
            return []

        if len(results) == 1:
            return results

        for result in results:

            return_list.append(int(result[0]))

        return return_list

    def get_methyl_probes_by_chromosome(self, chromosome):

        sql = 'select probe_name from methyl_probe_name_lookup where chrm=?'

        return_list = []

        results = self.execute_lookup((sql, [chromosome]))

        for result in results:

            return_list.append(result[0])

        return return_list

    def get_methyl_probe_info_by_name(self, probe_name):

        sql = 'select chrm, bp, probe_name from methyl_probe_name_lookup where probe_name = ?'

        result = self.execute_lookup((sql, [probe_name]))

        return result





