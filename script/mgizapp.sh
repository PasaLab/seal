#!/bin/bash
#
# This file is use mgiza++ do word alignment, It contains two part
#
# First is "mgiza", it generate two direction word alignment information, in this part you can set training configuration
#
# Second is script "merge_alignment.py", it will compress files generate in first part
#
#
# Author: YWJ
# Date： 2016-12-21
# Copyright (c) 2016 NJU PASA Lab All rights reserved.
# 

MGIZA=${QMT_HOME}/bin/mgiza
# This is your mgizapp install directory
if [ $# -lt 4 ]; then
    echo "OK, this is simple" 1>&2
    echo "run " $0 " PRE src tgt rootDir" 1>&2
    echo "first make sure this script cand find source and target file, and \${QMT_HOME} is in bashrc" 1>&2
    exit
fi

PRE=$1
SRC=$2
TGT=$3
ROOT=$4
NUM=$5

#mkdir -p $ROOT/giza.$SRC-$TGT
#mkdir -p giza.$TGT-$SRC

echo "Do First part : mgiza" 1>&2

${QMT_HOME}/bin/mgiza -ncpu 8 -c $ROOT/$PRE.${SRC}_$PRE.$TGT.snt -o $ROOT/giza-inverse.${NUM}/$SRC-${TGT} \
 -s $ROOT/$PRE.$SRC.vcb -t $ROOT/$PRE.$TGT.vcb -coocurrence $ROOT/giza-inverse.${NUM}/$SRC-${TGT}.cooc \
 -m1 7 -m2 0 -mh 8 - m3 3 -m4 3
 #-restart 11 -previoust $ROOT/giza-inverse.${NUM}/$SRC-$TGT.t3.final \
 #-previousa $ROOT/giza-inverse.${NUM}/$SRC-$TGT.a3.final -previousd $ROOT/giza-inverse.${NUM}/$SRC-$TGT.d3.final \
 #-previousn $ROOT/giza-inverse.${NUM}/$SRC-$TGT.n3.final -previousd4 $ROOT/giza-inverse.${NUM}/$SRC-$TGT.d4.final \
 #-previousd42 $ROOT/giza-inverse.${NUM}/$SRC-$TGT.D4.final -m3 0 -m4 1

${QMT_HOME}/bin/mgiza -ncpu 8 -c $ROOT/$PRE.${TGT}_$PRE.$SRC.snt -o $ROOT/giza.${NUM}/$TGT-${SRC} \
 -s $ROOT/$PRE.$TGT.vcb -t $ROOT/$PRE.$SRC.vcb -coocurrence $ROOT/giza.${NUM}/$TGT-${SRC}.cooc \
 -m1 7 -m2 0 -mh 8 - m3 3 -m4 3
 #-restart 11 -previoust $ROOT/giza-inverse.${NUM}/$SRC-$TGT.t3.final \
 #-previousa $ROOT/giza-inverse.${NUM}/$SRC-$TGT.a3.final -previousd $ROOT/giza-inverse.${NUM}/$SRC-$TGT.d3.final \
 #-previousn $ROOT/giza-inverse.${NUM}/$SRC-$TGT.n3.final -previousd4 $ROOT/giza-inverse.${NUM}/$SRC-$TGT.d4.final \
 #-previousd42 $ROOT/giza-inverse.${NUM}/$SRC-$TGT.D4.final -m3 0 -m4 1

echo "Done First part" 1>&2

echo "Do compression, merge_alignment.py ." 1>&2
${QMT_HOME}/scripts/merge_alignment.py $ROOT/giza-inverse.${NUM}/$SRC-${TGT}.A3.final.part* | gzip -c > $ROOT/giza-inverse.${NUM}/$SRC-$TGT.A3.final.gz

${QMT_HOME}/scripts/merge_alignment.py $ROOT/giza.${NUM}/$TGT-${SRC}.A3.final.part* | gzip -c > $ROOT/giza.${NUM}/$TGT-$SRC.A3.final.gz 

#${QMT_HOME}/scripts/giza2bal.pl -d "gzip -cd $ROOT/giza.${NUM}/$TGT-$SRC.A3.final.gz" -i "gzip -cd $ROOT/giza-inverse.${NUM}/$SRC-$TGT.A3.final.gz" | ${QMT_HOME}/bin/symal -a="grow" -d="yes" -f="yes" -b="yes" > $ROOT/grow-diag-final-and

echo "Done Second part!" 1>&2


