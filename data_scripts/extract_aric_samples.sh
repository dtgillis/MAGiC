#!/bin/bash

supposed_samples=`wc -l gwas_extract_sample_list.txt`
for file in chr9.ped.gz
do
	echo "-------------------------------------------------------"
	
	unzipped_ped=${file%.ped.gz}
	echo "Processing file $file for $unzipped_ped"
	zcat $file > $unzipped_ped.ped 
	sed 's%/% %g' $unzipped_ped.ped > $unzipped_ped.tab
	rm $unzipped_ped.ped
	#mv $unzipped_ped.tab $unzipped_ped.ped
	#echo $unzipped_ped
	grep -f gwas_extract_sample_list.txt $unzipped_ped.tab > $unzipped_ped.extract.ped
	mv *.nosex > log/ 
	total_samples=`wc -l $unzipped_ped.extract.ped`
	echo "Extracted $total_samples of $supposed_samples from $file"
	snp_count=`wc -l $unzipped_ped.map`
	echo "Total SNPs for chrm $unzipped_ped $snp_count"
	gzip ${unzipped_ped}.extract.ped
	rm $unzipped_ped.tab
	echo "---------------------------------------------------------"
done;
