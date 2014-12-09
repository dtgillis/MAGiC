__author__ = 'dtgillis'

import argparse
import ConfigParser
import os

from magic.database.sqlite_wrapper import SqliteConnector
from magic.database.sqlite_wrapper import SqliteLookup
from machine_learning.data_builder import GeMesDataSetFactory
from magic.methylation.methylation_reader import MethylationParser
from magic.plink.plink_wrapper import PlinkExecutableWrapper
from magic.machine_learning.learning import LogisticRegressor
import sys
import time


def run_logistic_regression():

    parser = argparse.ArgumentParser(description='Impute some genotype classes')
    parser.add_argument('--config_file', type=str, nargs=1, help='config file for building database')
    args = parser.parse_args()

    config = ConfigParser.ConfigParser()
    config.readfp(open(args.config_file[0], 'r'))

    if not config.has_section("Main"):
        print "improper config file no section \"Main\" in file {0:s}".format(args.config_file[0])
        exit()
    if not config.has_section("Data Files"):
        print "improper config file no section \"Data Files\" in file {0:s}".format(args.config_file[0])
        exit()
    work_dir = config.get("Main", "work_dir")
    if not os.path.exists(work_dir):
        os.makedirs(work_dir)

    beta_file = config.get("Data Files", "beta_file_directory") + os.sep + config.get("Data Files", "beta_file")

    if not os.path.isfile(beta_file):
        print 'Beta File {0:s} does not exist'.format(beta_file)

    if config.has_option("Plink", "plink_exec"):
        plink_exec = PlinkExecutableWrapper(plink_executable_path=config.get("Plink", "plink_exec"))
    else:
        plink_exec = PlinkExecutableWrapper()
    plink_dir = config.get("Data Files", "plink_file_directory")
    plink_ped = config.get("Data Files", "plink_file")
    plink_map = config.get("Data Files", "plink_map_file")

    map_file = plink_dir + os.sep + plink_map
    ped_file = plink_dir + os.sep + plink_ped

    db_connect = SqliteConnector(work_dir + os.sep + config.get("Main", "database_name"))
    sqlite_seeker = SqliteLookup(db_connect)
    methyl_parser = MethylationParser(beta_file)
    snp_list = sqlite_seeker.get_distinct_gemes_snps()
    snp_count = 0
    start_time = time.clock()
    for snp_tuple in snp_list:
        if snp_count % 10 == 0 and snp_count != 0:
            sys.stdout.write(".")
        if snp_count % 100 == 0 and snp_count != 0:
            print "((((((((((((((((())))))))))))))))))))"
            print "% Processed {0:d} snps of {1:d} %".format(snp_count, len(snp_list))
            print "% in {0:d} secs of compute time %".format(int(time.clock() - start_time))
            print "((((((((((((((((())))))))))))))))))))"



        #TODO implement an actual tmp directory choice in cfg
        gemes_factory = GeMesDataSetFactory(db_connect, methyl_parser, plink_exec, ped_file, map_file, work_dir)
        y,X = gemes_factory.get_gemes_data_set(snp_tuple[-1])
        if len(y) > 0:
            log_regressor = LogisticRegressor()
            pearson_r_2, best_params = log_regressor.grid_search(X, y)
            out_file = open(work_dir + os.sep + "log_reg_gemes.dat", 'a')
            out_file.write('{0:s} {1:f} penalty {2:s} C {3:f}{4:s}'.format(
                snp_tuple[-1], pearson_r_2, best_params['penalty'], best_params['C'], os.linesep))
            out_file.close()

if __name__ == '__main__':

    run_logistic_regression()
