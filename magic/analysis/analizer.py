__author__ = 'dtgillis'

import os
import matplotlib.pyplot as plt
from magic.database.sqlite_wrapper import SqliteConnector
from magic.machine_learning.data_builder import ChromeWideMethylationDataSet
from magic.plink.plink_wrapper import PlinkExecutableWrapper
from magic.machine_learning.learning import LogisticRegressor
import numpy as np
from magic.database.sqlite_wrapper import SqliteLookup

class ResultWithGridParams():

    def __init__(self, snp_name, r_squared, params):

        self.params = params
        self.snp_name = snp_name
        self.r_squared = r_squared


class ResultParser():

    def __init__(self, result_file, cutoff=.0):

        self.result_file = result_file
        self.result_dict = dict()
        self.cutoff = cutoff

    def parse_result_file(self):

        self.result_dict = dict()

        for line in open(self.result_file, 'r'):

            fields = line.strip(os.linesep).split()

            if len(fields) > 2:
                if float(fields[1]) >= self.cutoff:
                    param_dict = dict()
                    num_params = len(fields[2:])/2
                    for i in range(num_params):
                        param_dict[fields[2*i+2]] = fields[2*i+3]

                    self.result_dict[fields[0]] = ResultWithGridParams(
                        snp_name=fields[0], r_squared=float(fields[1]),
                        params=param_dict
                    )

    def get_parsed_results(self, cutoff=.0):

        if len(self.result_dict) == 0 or self.cutoff != cutoff:
            self.cutoff = cutoff
            self.parse_result_file()
            return self.result_dict
        else:
            return self.result_dict


def histogram_r_squared(results, x_lab=None, y_lab=None, title=None, figure_save='fig.png'):

    r_squared_list = []

    for snp_name, result in results.items():
        r_squared_list.append(result.r_squared)

    plt.hist(r_squared_list, bins=100)
    plt.xlabel(x_lab)
    plt.ylabel(y_lab)
    plt.title(title)
    plt.savefig(figure_save, bbox_inches='tight')
    plt.show()

def plot_coefficients_for_snp(db_connection, methyl_parser, plink_executable,
                              ped_file, map_file, tmp_dir, snp_result_list):

    #assert isinstance(db_connection, SqliteConnector)
    #assert isinstance(plink_executable, PlinkExecutableWrapper)

    for snp_name, snp_result in snp_result_list.items():

        machine_data = ChromeWideMethylationDataSet(db_connection=db_connection, methyl_parser=methyl_parser,
                                                    plink_executable=plink_executable, plink_file=ped_file,
                                                    plink_map=map_file, tmp_dir=tmp_dir)

        y, X, probe_names = machine_data.get_chrome_wide_methyl_probes_by_snp_name(snp_result.snp_name)

        log_reg = LogisticRegressor(params=snp_result.params)

        pearson_r, coeff = log_reg.fit_data(y, X)

        avg_r = np.mean(pearson_r)

        sqlite_seeker = SqliteLookup(db_connection)
        plot_dict = dict()
        # get x coordinates
        plot_x = []
        snp_info = sqlite_seeker.snp_info_lookup(snp_name)
        gemes_probes = sqlite_seeker.get_methyl_probes_in_geme_pair_by_snp_id(snp_info[0])
        gemes_list = []
        for methyl_id in gemes_probes:

            gemes_list.append(sqlite_seeker.get_methyl_probe_info_by_id(methyl_id))

        for i, probe in enumerate(probe_names):

            probe_info = sqlite_seeker.get_methyl_probe_info_by_name(probe)
            plot_x.append(int(probe_info[1]))
        plot_x = np.array(plot_x)
        y_max = 0
        y_min = 0
        for group, coeff_array in coeff.items():
            avg_coeff = np.mean(coeff_array, axis=0)
            y_max = max(y_max, np.max(avg_coeff))
            y_min = min(y_min, np.min(avg_coeff))
            plot_dict[group] = avg_coeff


        for group, coeff_vec in plot_dict.items():
            probes = np.where(coeff_vec != .0)
            if len(probes) != 0:
                plt.plot(plot_x[probes[0]], coeff_vec[probes[0]], '.', label='Group {0:d}'.format(group))

        plt.vlines(snp_info[2], ymin=y_min, ymax=y_max)
        for geme in gemes_list:

            if geme[0] == u'9':
                plt.vlines(geme[1], ymin=y_min, ymax=y_max, linestyles=u'dashed')
        plt.legend()


        plt.show()