__author__ = 'dtgillis'

import os


class SampleGenotype():

    def __init__(self, sample_list, genotype_list, snp_name):

        self.sample_list = sample_list
        self.genotype_list = genotype_list
        self.snp_name = snp_name


class PlinkExecutableWrapper():

    def __init__(self, plink_executable_path=None):

        self.plink_found = False
        if plink_executable_path is None:
            for path in os.environ["PATH"].split(os.pathsep):

                if os.path.isfile(path + os.sep + 'plink'):
                    self.plink_exec = path + os.sep + 'plink'
                    self.plink_found = True
                    print self.plink_exec
        else:
            if os.path.isfile(plink_executable_path):
                self.plink_found = True
                self.plink_exec = plink_executable_path

        if self.plink_found:
            print "Found plink executable"

    def extract_snps_recode(self, map_file, ped_file, snp, tmp_dir):

        #form the command for the plink execution
        cmd = '{0:s} --noweb --ped {1:s} --map {2:s} --map3 --no-pheno --recodeA --snp {3:s}' \
              ' --out {4:s}{5:s}{3:s}'.format(self.plink_exec, ped_file, map_file, snp, tmp_dir, os.sep)
        os.system(cmd)

    def parse_snp_recode_raw(self, snp, tmp_dir):

        recode_file = tmp_dir + os.sep + snp + '.raw'
        if not os.path.isfile(recode_file):
            print "error reading plink file {0:s}".format(recode_file)
            exit(1)

        lines = open(tmp_dir + os.sep + snp + '.raw', 'r').readlines()

        #splice out
        split_lines = [line.strip(os.linesep).split() for line in lines[1:]]
        genotype_list = []
        sample_list = []
        for record in split_lines:

            if record[-1] != 'NA':
                sample_list.append(record[0])
                genotype_list.append(int(record[-1]))


        sample_genotype = SampleGenotype(sample_list, genotype_list, snp)

        return sample_genotype

    def clean_up_directory(self, tmp_dir, snp):

        file_start = tmp_dir + os.sep + snp
        for ext in ['.raw', '.nof', '.log']:
            if os.path.isfile(file_start + ext):
                os.remove(file_start + ext)


