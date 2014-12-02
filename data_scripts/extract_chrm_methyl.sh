#!/bin/bash


for chrm in {1..22}
do
	echo "-----------------------------------------------"
	echo "Starting chrm $chrm"
	
	awk -v chrm=$chrm ' BEGIN { FS=",";} { if ( $3 == chrm ) print $1 ; } ' sorted.probe_name_loc.clean.csv > probe_names.$chrm


	#split probe files they are kinda big and take too much memory
	split -l 5000 -d probe_names.$chrm probe_names.$chrm.

	# get the first line with sample names
	head -n 1 beta.txt > methyl.beta.chrm.$chrm

	total_found=0
	# print out a line if it is on the right chromosome 
	for file in probe_names.$chrm.*
	do
		echo "Processing file : ${file}"
		records=`wc -l $file`
		echo "$records in ${file}"
		grep -f $file beta.txt >> methyl.beta.chrm.$chrm
		in_out_file=`wc -l methyl.beta.chrm.$chrm`
		echo "$in_out_file records in out file"
		

	done;
	echo "------------------------------------------------------------"
	echo "Finished processing betas for chrm $chrm"
	chrm_probes=`wc -l methyl.beta.chrm.$chrm`
	echo "Extracted $chrm_probes from chrm $chrm"
	rm probe_names.$chrm*
done;

echo "------------------------------------------------------"
echo "Overall Results"
total_probes=`wc -l methyl.beta.chrm.* | tail -n 1 | awk '{ print $1; }'`
echo "Total number of probes : $total_probes"
