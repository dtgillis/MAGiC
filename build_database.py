__author__ = 'dtgillis'

import database.sqlite_wrapper
from database.database_builder import DatabaseBuilder
import argparse


def build_database():
    parser = argparse.ArgumentParser(description='Impute some genotype classes')
    parser.add_argument('--work_dir', type=str, nargs=1, help='working directory')
    parser.add_argument('--build_script', type=str, nargs=1, help='path to build script for database')
    parser.add_argument('--database_name', type=str, nargs=1, help='Name given to the database')

    args = parser.parse_args()

    # get a builder object and set up the database
    builder = DatabaseBuilder(args.work_dir[0], args.database_name[0], args.build_script[0])
    builder.check_work_dir(create=True)
    builder.create_database(force=True)
    builder.get_build_script()
    builder.run_database_build_script()

if __name__ == '__main__':

    build_database()