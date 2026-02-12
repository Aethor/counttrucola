#!/bin/bash

# add split field to train/valid/test
fields_nb=$(awk '-F\t' '{ print NF; exit }' train.txt)
if [[ $fields_nb -eq 4 ]]; then
    sed -e 's/$/\t0/' -i train.txt 
    sed -e 's/$/\t1/' -i valid.txt 
    sed -e 's/$/\t2/' -i test.txt 
fi

# change \t to , to have CSV format
cat train.txt valid.txt test.txt | sed 's/\t/,/g' > tkgl-yago4.5_edgelist.csv

# correctly re-order fields (yes, a specific order is expected even though this is a csv)
cat tkgl-yago4.5_edgelist.csv | awk 'BEGIN{FS=OFS=","}{print $4,$1,$3,$2,$5}' > tkgl-yago4.5_edgelist.csv.$$
mv tkgl-yago4.5_edgelist.csv.$$ tkgl-yago4.5_edgelist.csv 

# # indicate fields
# # [ timestamps head tail relation_type split ]
sed -i '1itimestamps,head,tail,relation_type,split' tkgl-yago4.5_edgelist.csv 
