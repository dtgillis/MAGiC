__author__ = 'dtgillis'

import sqlite_wrapper
import os


class DatabaseBuilder():
    """
    Class used to setup the database build config_files and run them. Also will create the database
    """
    def __init__(self, work_dir, database_name, database_build_script):

        self.work_dir = work_dir
        self.database_name = database_name
        self.build_script = database_build_script

    def check_work_dir(self, create=False):

        if not os.path.exists(self.work_dir):

            if create:
                try:
                    os.makedirs(self.work_dir)
                except Exception as e:
                    print e.message
                    print "Could not create working directory {0:s}".format(self.work_dir)
            else:
                print "Work directory {0:s} does not exist and create was not enabled".format(self.work_dir)

    def get_build_script(self):

        if os.path.isfile(self.build_script):

            build_script_file = open(self.build_script, 'r')
            build_script = build_script_file.read()
            build_script_file.close()
            return build_script
        else:
            print "Build Script {0:s} is not a valid file to read".format(self.build_script)
            exit(1)

    def get_database_writer(self):

        db_name = self.work_dir + os.sep + self.database_name
        sqlite_connection = sqlite_wrapper.SqliteConnector(db_name=db_name, write_privelage=True)
        sqlite_writer = sqlite_wrapper.SqliteWriter(sqlite_connection)

        return sqlite_writer

    def get_database_seeker(self):

        db_name = self.work_dir + os.sep + self.database_name
        sqlite_connection = sqlite_wrapper.SqliteConnector(db_name=db_name, write_privelage=True)
        sqlite_seeker = sqlite_wrapper.SqliteLookup(sqlite_connection)

        return sqlite_seeker

    def create_database(self, force=False):

        if os.path.isfile(self.work_dir + os.sep + self.database_name) and not force:

            print "Database {0:s} already exists at {1:s} please use force=False to overwrite it".format(
                (self.database_name, self.work_dir))
        else:
            db_name = self.work_dir + os.sep + self.database_name

            if os.path.isfile(db_name) and force:
                #dump the database
                #TODO make a backup of the database as well
                os.remove(db_name)
            # create a new database
            sqlite_connect = sqlite_wrapper.SqliteConnector(db_name=db_name, write_privelage=True)
            sqlite_connect.connect_to_database()
            sqlite_connect.close_connection_to_database()

    def run_database_build_script(self):

        print "Parsing Build Script"
        build_script = self.get_build_script()
        db_name = self.work_dir + os.sep + self.database_name
        sqlite_connect = sqlite_wrapper.SqliteConnector(db_name=db_name, write_privelage=True)
        print "Executing Build Script"
        sqlite_connect.execute_sql_script(build_script)
        print "Finished Build Script"
        sqlite_connect.close_connection_to_database()

    def fill_gwas_methyl_lookup_table(self, gwas_methyl_map):

        sqlite_writer = self.get_database_writer()

        if not os.path.isfile(gwas_methyl_map):
            print "Gwas methyl mapping file {0:s} does not exist".format(gwas_methyl_map)
            exit(1)

        gwas_methyl_mapping = [line.strip(os.linesep).split() for line in open(gwas_methyl_map, 'r')]

        sql = "insert into gwas_methyl_lookup (methyl_sample_id, gwas_sample_id) values (?,?)"

        sqlite_writer.execute_statement((sql, gwas_methyl_mapping), multiple_statements=True)

    def fill_methyl_probe_name_lookup(self, beta_file):

        sqlite_writer = self.get_database_writer()

        methyl_probe_names = [[line.strip(os.linesep).split()[0].replace("\"", '')] for line in open(beta_file, 'r')]

        sql = "insert into methyl_probe_name_lookup (probe_name) values(?)"

        sqlite_writer.execute_statement((sql, methyl_probe_names[1:]), multiple_statements=True)

    def fill_snp_name_lookup(self, plink_map_file):

        sqlite_writer = self.get_database_writer()

        snp_names = [[line.strip(os.linesep).split()[1]] for line in open(plink_map_file, 'r')]

        sql = "insert into snp_name_lookup (snp_name) values(?)"

        sqlite_writer.execute_statement((sql, snp_names), multiple_statements=True)

    def fill_gemes_table(self, gemes_file):

        sqlite_writer = self.get_database_writer()

        sqlite_seeker = self.get_database_seeker()

        gemes_records = []

        print "reading in gemes"

        for line in open(gemes_file, 'r'):

            fields = line.strip().split(',')

            #sql for methyl probe name
            methyl_probe_name_sql = 'select id from methyl_probe_name_lookup where probe_name = ?'

            methyl_probe_result = sqlite_seeker.execute_lookup((methyl_probe_name_sql, [fields[0]]))

            if methyl_probe_result is not None:
                methyl_probe_id = int(methyl_probe_result[0])
            else:
                continue
            snp_name_sql = 'select id from snp_name_lookup where snp_name = ?'

            snp_name_result = sqlite_seeker.execute_lookup((snp_name_sql, [fields[3]]))

            if snp_name_result is not None:
                snp_name_id = int(snp_name_result[0])
            else:
                continue

            gemes_records.append((methyl_probe_id, snp_name_id))

        gemes_sql = 'insert into gemes(probe_id,snp_id) values (?,?)'
        sqlite_writer.execute_statement((gemes_sql, gemes_records), multiple_statements=True)














