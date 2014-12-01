__author__ = 'dtgillis'

import sqlite_wrapper
import os


class DatabaseBuilder():
    """
    Class used to setup the database build scripts and run them. Also will create the database
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
        db_name = self.work_dir + os.sep + self.build_script
        sqlite_connect = sqlite_wrapper.SqliteConnector(db_name=db_name, write_privelage=True)
        print "Executing Build Script"
        sqlite_connect.execute_sql_script(build_script)
        print "Finished Build Script"





