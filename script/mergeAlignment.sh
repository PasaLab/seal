#!/bin/bash
#
# This file is use mgiza++ merge two direction word alignment.
#
# script is symal and giza2bal.py, use method "grow-diag-final-and"
#
#
# Author: YWJ
# Date： 2016-12-21
# Copyright (c) 2016 NJU PASA Lab All rights reserved.
# 

MGIZA=${QMT_HOME}/bin/mgiza
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


echo "Do giza2bal.py & symal" 1>&2
#${QMT_HOME}/scripts/merge_alignment.py $ROOT/giza-inverse.${NUM}/$SRC-${TGT}.A3.final.part* | gzip -c > $ROOT/giza-inverse.${NUM}/$SRC-$TGT.A3.final.gz

#${QMT_HOME}/scripts/merge_alignment.py $ROOT/giza.${NUM}/$TGT-${SRC}.A3.final.part* | gzip -c > $ROOT/giza.${NUM}/$TGT-$SRC.A3.final.gz 

${QMT_HOME}/scripts/giza2bal.pl -d "gzip -cd $ROOT/giza.${NUM}/$TGT-$SRC.A3.final.gz" -i "gzip -cd $ROOT/giza-inverse.${NUM}/$SRC-$TGT.A3.final.gz" | ${QMT_HOME}/bin/symal -a="grow" -d="yes" -f="yes" -b="yes" > $ROOT/grow-diag-final-and.${NUM}

echo "Done" 1>&2

cat $ROOT/grow-diag-final-and.${NUM} | sed -e '/ALIGN_ERR/d' |  sed 's/{##}/\t/g' > ${ROOT}/${PRE}.clean.tmp.${NUM}
rm -rf $ROOT/grow-diag-final-and.${NUM}
mv ${ROOT}/${PRE}.clean.tmp.${NUM} $ROOT/grow-diag-final-and.${NUM}

#rm -rf $ROOT/giza-inverse.${NUM}
#rm -rf $ROOT/giza.${NUM}
#rm -rf $ROOT/prepared.${NUM} 
