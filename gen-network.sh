#!/usr/bin/env bash
##########################
# Student ID:   D10126532
# Student Name: John Warde
# Course Code:  DT230B
# 

#hadoop jar GenerateCoAppearanceNetwork.jar GenerateCoAppearanceNetwork -D match-within-n-lines=10 plays/RomeoJuliet.txt $1
DESTDIR=~/OnPC/Hadoop
hadoop fs -get $1/part-00000 $DESTDIR/$1.csv
echo Source Target  State   Weight > $DESTDIR/gephi_$1.csv
sed -e "s/undirected\tundirected/undirected/" <$DESTDIR/$1.csv >>$DESTDIR/gephi_$1.csv
#sed "s/BENVOLIO/JB/" <$DESTDIR/$1.csv >$DESTDIR/gephi_$1.csv
cat $DESTDIR/gephi_$1.csv

