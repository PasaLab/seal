#!/bin/bash
#
# This file is part of word alignment, It will do three part

# First is "plaint2snt", it generate two kind of  format files : *.vcb and *.snt

# Second is "sntcooc", it generate one kind of format file "*cooc"

# Third is "mkcls", it will generate one kind of format file ".classes"
#
# Author: YWJ
# Date： 2016-12-21
# Copyright (c) 2016 NJU PASA Lab All rights reserved.
# 

MGIZA=${QMT_HOME}/bin/mgiza
if [ $# -lt 4 ]; then
    echo "OK, this is simple, needs four parameters" 1>&2
    echo "PRE is prefix of filename, SRC is source , TGT is target, root is your workspace, " 1>&2
    echo "run " $0 " PRE src tgt rootDir" 1>&2
    echo "first make sure this script cand find source and target file, and \${QMT_HOME} is in bashrc" 1>&2
    exit
fi

PRE=$1
SRC=$2
TGT=$3
ROOT=$4
NUM=$5

mkdir -p $ROOT/giza-inverse.${NUM}
mkdir -p $ROOT/giza.${NUM}
mkdir -p $ROOT/prepared.${NUM}

echo "Do First part : plain2snt" 1>&2
${QMT_HOME}/bin/plain2snt $ROOT/$PRE.$SRC $ROOT/$PRE.$TGT

echo "Done First part" 1>&2

echo "Do Second part : snt2cooc" 1>&2
${QMT_HOME}/bin/snt2cooc $ROOT/giza.${NUM}/$TGT-$SRC.cooc $ROOT/$PRE.$SRC.vcb $ROOT/$PRE.$TGT.vcb $ROOT/$PRE.$TGT\_$PRE.$SRC.snt
${QMT_HOME}/bin/snt2cooc $ROOT/giza-inverse.${NUM}/$SRC-$TGT.cooc $ROOT/$PRE.$TGT.vcb $ROOT/$PRE.$SRC.vcb $ROOT/$PRE.$SRC\_$PRE.$TGT.snt 
echo "Done Second part!" 1>&2

echo "Do Third part: mkcls" 1>&2
${QMT_HOME}/bin/mkcls -c50 -n5 -p$ROOT/$PRE.$SRC -V$ROOT/$PRE.$SRC.vcb.classes opt
${QMT_HOME}/bin/mkcls -c50 -n5 -p$ROOT/$PRE.$TGT -V$ROOT/$PRE.$TGT.vcb.classes opt
echo "Done Third" 1>&2
