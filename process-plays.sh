#!/usr/bin/env bash
##########################
# Student ID:   D10126532
# Student Name: John Warde
# Course Code:  DT230B
# 

SRCDIR=$1
JARDIR=~/workspace/D10126532/bin
HDFSIN=/user/soc/autoplaysin
HDFSOUT=/user/soc/autoplaysout

FILES=$SRCDIR/*
for f in $FILES
do
  FILENAME=`basename $f .txt`  
  echo "Processing $FILENAME play ..."
  hadoop fs -put $SRCDIR/$FILENAME.txt $HDFSIN
  hadoop jar $JARDIR/GenerateCoAppearanceNetwork.jar \
         literaryanalysis.GenerateCoAppearanceNetwork \
         -D match-within-n-lines=5 \
         $HDFSIN/$FILENAME.txt \
         $HDFSOUT/$FILENAME.csv &
done

