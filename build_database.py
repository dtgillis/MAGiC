__author__ = 'dtgillis'

from database.database_builder import DatabaseBuilder
import argparse
import ConfigParser
import os
from methylation.methylation_reader import MethylationParser


def build_database():

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

    # get a builder object and set up the database
    builder = DatabaseBuilder(config.get("Main", "work_dir"), config.get("Main", "database_name"),
                              config.get("Main", "build_script"))
    builder.check_work_dir(create=True)
    builder.create_database(force=True)
    builder.run_database_build_script()

    print "Inserting records into gwas_methyl_lookup table"
    builder.fill_gwas_methyl_lookup_table(config.get("Data Files", "methyl_gwas_mapping"))
    print "Inserting records into methyl probe name lookup"
    beta_file = config.get("Data Files", "beta_file_directory") + os.sep + config.get("Data Files", "beta_file")
    builder.fill_methyl_probe_name_lookup(beta_file)
    print "Inserting records into snp name lookup"
    plink_map_file = config.get("Data Files", "plink_file_directory") + os.sep + config.get("Data Files", "plink_map_file")
    builder.fill_snp_name_lookup(plink_map_file)
    builder.fill_gemes_table(config.get("Data Files", "gemes_file"))

    beta_file_path = config.get("Data Files", "beta_file_directory") + os.sep + config.get("Data Files", "beta_file")

    methylation_parser = MethylationParser(beta_file_path)

    methylation_parser.create_methylation_file_index()


if __name__ == '__main__':

    build_database()