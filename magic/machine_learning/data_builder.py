__author__ = 'dtgillis'

import os

import numpy as np

from magic.database.sqlite_wrapper import SqliteConnector
from magic.database.sqlite_wrapper import SqliteLookup
from magic.methylation.methylation_reader import MethylationParser
from magic.plink.plink_wrapper import PlinkExecutableWrapper


class MethylData():

    def __init__(self, probe_name, probe_data, sample_list):

        self.probe_name = probe_name
        self.probe_data = probe_data
        self.sample_list = sample_list


class MethylProbeDataFactory():

    def __init__(self, probe_list, methyl_parser):

        assert isinstance(methyl_parser, MethylationParser)

        self.parser = methyl_parser
        self.probe_list = probe_list

    def get_probe_data(self):

        return_data = []

        for probe in self.probe_list:

            tmp_data = self.parser.get_probe_data_by_name(probe)

            if tmp_data is not None:

                fields = tmp_data.strip(os.sep).replace('\"', '').split()

                sample_list = self.parser.get_methylation_sample_names()

                return_data.append(MethylData(fields[0], fields[1:], sample_list))

        return return_data


class GemesSnpFactory():

    def __init__(self, db_connection):

        assert isinstance(db_connection, SqliteConnector)
        self.db_connect = db_connection
        self.sqlite_seeker = SqliteLookup(self.db_connect)

    def get_gemes_snps_in_db(self):

        snp_list = self.sqlite_seeker.get_distinct_gemes_snps()

        return snp_list


class GenotypeDataFactory():

    #TODO should be extended to only a given chromosome
    def __init__(self, plink_executable, plink_file, plink_map, tmp_dir):

        assert isinstance(plink_executable, PlinkExecutableWrapper)
        self.plink_executable = plink_executable

        self.tmp_dir = tmp_dir
        self.plink_file = plink_file
        self.plink_map = plink_map

    def get_snp_data(self, snp):

        self.plink_executable.extract_snps_recode(self.plink_map, self.plink_file, snp, self.tmp_dir)
        sample_genotype = self.plink_executable.parse_snp_recode_raw(snp, self.tmp_dir)
        self.plink_executable.clean_up_directory(self.tmp_dir, snp)

        return sample_genotype


class MachineLearningData():

    def __init__(self, methyl_probe_data, snp_data, db_connection):

        assert isinstance(db_connection, SqliteConnector)
        self.db_connect = db_connection
        self.sqlite_seeker = SqliteLookup(self.db_connect)
        self.methyl_probe_data = methyl_probe_data
        self.snp_data = snp_data

    def pair_methyl_gwas_data(self, clean_missing=True):

        gwas_dict = dict()
        gwas_set = set()
        methyl_dict = dict()
        methyl_set = set()
        if clean_missing:

            sample_count = 0
            for sample in self.snp_data.sample_list:

                gwas_methyl_id = self.sqlite_seeker.gwas_methyl_lookup_table_search(sample, by_gwas=True)

                if gwas_methyl_id is not None:

                    gwas_dict[gwas_methyl_id] = sample_count
                    gwas_set.add(gwas_methyl_id)

                sample_count += 1

            if len(self.methyl_probe_data) > 0:

                sample_count = 0
                for sample in self.methyl_probe_data[0].sample_list:

                    gwas_methyl_id = self.sqlite_seeker.gwas_methyl_lookup_table_search(sample, by_gwas=False)

                    if gwas_methyl_id is not None:

                        methyl_dict[gwas_methyl_id] = sample_count
                        methyl_set.add(gwas_methyl_id)
                    sample_count += 1

                in_both_sets = methyl_set.intersection(gwas_set)
                # get list of samples in both lists
                gwas_list = [[key, gwas_dict[key]] for key in list(in_both_sets)]
                methyl_list = [[key, methyl_dict[key]] for key in list(in_both_sets)]
                # sort the lists and get indices
                sorted_gwas_list = sorted(gwas_list, key=lambda items: items[0])
                sorted_methyl_list = sorted(methyl_list, key=lambda items: items[0])
                gwas_index = [index[-1] for index in sorted_gwas_list]
                methyl_index = [index[-1] for index in sorted_methyl_list]
                # print self.methyl_probe_data[0].sample_list[methyl_index[0]]
                # print self.snp_data.sample_list[gwas_index[0]]
                return gwas_index, methyl_index
            else:
                print "No Probe Data Supplied?"


    def get_snp_as_Y(self, gwas_index):

        numpy_genotype = np.array(self.snp_data.genotype_list)

        return numpy_genotype[gwas_index]

    def get_methyl_as_X(self, methyl_index):

        probe_list = []

        n_cols = len(self.methyl_probe_data)

        methyl_np = np.zeros((len(methyl_index), n_cols))

        col_num = 0

        for methyl_data in self.methyl_probe_data:

            methyl_np[:, col_num] = np.array(methyl_data.probe_data)[methyl_index]
            probe_list.append(methyl_data.probe_name)
            col_num += 1

        return methyl_np, probe_list


class GeMesDataSetFactory():

    def __init__(self, db_connection, methyl_parser, plink_executable, plink_file, plink_map, tmp_dir):

        assert isinstance(db_connection, SqliteConnector)
        assert isinstance(methyl_parser, MethylationParser)
        assert isinstance(plink_executable, PlinkExecutableWrapper)
        self.db_connect = db_connection
        self.sqlite_seeker = SqliteLookup(self.db_connect)
        self.methyl_parser = methyl_parser
        self.plink_executable = plink_executable
        self.plink_file = plink_file
        self.tmp_dir = tmp_dir
        self.plink_map = plink_map


    def get_gemes_data_set(self, snp_name):

        snp_id = self.sqlite_seeker.snp_id_lookup(snp_name)

        if snp_id is not None:

            methyl_ids = self.sqlite_seeker.get_methyl_probes_in_geme_pair_by_snp_id(snp_id)

            probe_names = []

            for methyl_id in methyl_ids:

                probe_names.append(self.sqlite_seeker.methyl_probe_name_lookup(methyl_id))

            probe_factory = MethylProbeDataFactory(probe_names, self.methyl_parser)

            probe_data_list = probe_factory.get_probe_data()

            snp_factory = GenotypeDataFactory(self.plink_executable, self.plink_file, self.plink_map, self.tmp_dir)

            snp_data = snp_factory.get_snp_data(snp_name)

            machine_data = MachineLearningData(probe_data_list, snp_data, self.db_connect)

            gwas_index, methyl_index = machine_data.pair_methyl_gwas_data()

            y = machine_data.get_snp_as_Y(gwas_index)
            X, probe_list = machine_data.get_methyl_as_X(methyl_index)

            return y, X
        else:
            return None

    #TODO make dataset abstract class to inherit


class ChromeWideMethylationDataSet():

    def __init__(self, db_connection, methyl_parser, plink_executable, plink_file, plink_map, tmp_dir):

        assert isinstance(db_connection, SqliteConnector)
        assert isinstance(methyl_parser, MethylationParser)
        assert isinstance(plink_executable, PlinkExecutableWrapper)
        self.db_connect = db_connection
        self.sqlite_seeker = SqliteLookup(self.db_connect)
        self.methyl_parser = methyl_parser
        self.plink_executable = plink_executable
        self.plink_file = plink_file
        self.tmp_dir = tmp_dir
        self.plink_map = plink_map

    def get_chrome_wide_methyl_probes_by_snp_name(self, snp_name):

        snp_desc = self.sqlite_seeker.snp_info_lookup(snp_name)

        probe_names = self.sqlite_seeker.get_methyl_probes_by_chromosome(snp_desc[1])

        probe_factory = MethylProbeDataFactory(probe_names, self.methyl_parser)

        probe_data_list = probe_factory.get_probe_data()

        snp_factory = GenotypeDataFactory(self.plink_executable, self.plink_file, self.plink_map, self.tmp_dir)

        snp_data = snp_factory.get_snp_data(snp_name)

        machine_data = MachineLearningData(probe_data_list, snp_data, self.db_connect)

        gwas_index, methyl_index = machine_data.pair_methyl_gwas_data()

        y = machine_data.get_snp_as_Y(gwas_index)

        X, probe_names_out = machine_data.get_methyl_as_X(methyl_index)


        return y, X, probe_names_out