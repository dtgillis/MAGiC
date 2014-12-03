__author__ = 'dtgillis'

import os
import pickle


class MethylationParser():

    def __init__(self, methylation_file,):

        self.data_file = methylation_file
        self.idx_file = methylation_file + '.idx'
        self.data_idx = self.load_methylation_file_index()
        self.data_file_fp = None

    def get_methylation_sample_names(self):

        beta_file = open(self.data_file, 'r')

        line_1 = beta_file.readline()

        samples = line_1.strip(os.linesep).replace('\"', '').split()

        return samples

    def read_methylation_probe_info(self):

        beta_fp = open(self.data_file, 'r')

        #first line is sample names so burn it
        beta_fp.readline()

        # set to second line
        return beta_fp

    def create_methylation_file_index(self):

        idx_dict = dict()

        with open(self.data_file) as fp:
            tmp_seek = 0
            for line in iter(fp.readline, ''):
                fields = line.strip(os.linesep).replace('\"', '').split()
                idx_dict[fields[0]] = tmp_seek
                tmp_seek = fp.tell()

        pickle.dump(idx_dict, open(self.data_file + '.idx', 'wb'))

    def load_methylation_file_index(self):


        if not os.path.isfile(self.idx_file):
            self.create_methylation_file_index()

        return pickle.load(open(self.idx_file, 'rb'))

    def get_fp_beta_file(self):

        if self.data_file_fp is None:

            self.data_file_fp = open(self.data_file, 'r')

        return self.data_file_fp

    def get_probe_data_by_name(self, probe_name):

        if probe_name in self.data_idx:
            fseek = self.data_idx[probe_name]

            probe_line = self.get_fp_beta_file().seek(fseek).readline()

            return probe_line

        else:

            return None