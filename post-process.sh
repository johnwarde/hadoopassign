#!/usr/bin/env bash
##########################
# Student ID:   D10126532
# Student Name: John Warde
# Course Code:  DT230B
# 

FILENAME=$1
DESTDIR=$2

# Retrieve the file from HDFS and save locally
hadoop fs -get $FILENAME/part-00000 $DESTDIR/$FILENAME.csv

# The following streaming editor (sed) command 
# will insert a header row at the top of the 
# file and will replace the repeated "undirected" 
# word with only a single occurance.
sed -e "1 iSource\tTarget\tState\tWeight" -e "s/undirected\tundirected/undirected/" <$DESTDIR/$FILENAME.csv >$DESTDIR/gephi_$FILENAME.csv
echo Output file is $DESTDIR/gephi_$FILENAME.csv

