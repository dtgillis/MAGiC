__author__ = 'dtgillis'

import ConfigParser
import argparse
from analysis import analizer
from magic.plink.plink_wrapper import PlinkExecutableWrapper
from magic.methylation.methylation_reader import MethylationParser
import os
from magic.database.sqlite_wrapper import SqliteConnector
from magic.database.sqlite_wrapper import SqliteLookup

def make_hist():

    parser = argparse.ArgumentParser(description='Impute some genotype classes')
    parser.add_argument('--config_file', type=str, nargs=1, help='config file for building database')
    args = parser.parse_args()

    config = ConfigParser.ConfigParser()
    config.readfp(open(args.config_file[0], 'r'))

    work_dir = config.get("Main", "work_dir")

    result_file = config.get("Analysis", "result_file")

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

    analysis = analizer.ResultParser(result_file)

    analizer.histogram_r_squared(analysis.get_parsed_results(),
                                 'R squared', 'Frequency',
                                 'Grid Searched Logistic Regression', config.get("Analysis", "hist_out_file"))

    snp_result_list = analysis.get_parsed_results(cutoff=.7)

    analizer.plot_coefficients_for_snp(db_connect, methyl_parser, plink_exec, ped_file, map_file, work_dir, snp_result_list)







if __name__ == '__main__':

    make_hist()