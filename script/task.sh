#!/bin/bash
#
# Author: YWJ
# Date： 2016-12-21
# Copyright (c) 2016 NJU PASA Lab All rights reserved

#10w step1
echo "**********10w step 1**********"
(time /home/experiment/ywj/singleSMT/data/prepare.sh UNPC.10w zh en /home/experiment/ywj/singleSMT/data/training 1 > pro_10w.log) 2> pro_10w.md
echo "**********Done 10w step 1*********"
echo "**********10w step 2**********"
#(time /home/experiment/ywj/singleSMT/data/mgizapp.sh UNPC.10w zh en /home/experiment/ywj/singleSMT/data/training 1 > mgizapp_10w.log) 2> mgizapp_10w.md
echo "**********Done 10w step 2**********"
echo "**********10w step 3**********"
#(time /home/experiment/ywj/singleSMT/data/mergeAlignment.sh UNPC.10w zh en /home/experiment/ywj/singleSMT/data/training 1 > merge_10w.log) 2> merge_10w.md
echo "**********Done 10w step 3**********"

echo "**********50w step1**********"
#time /home/experiment/ywj/singleSMT/data/prepare.sh UNPC.50w zh en /home/experiment/ywj/singleSMT/data/training 2 > pro_50w.log 2> pro_50w.md
echo "**********Done 50w step 1**********"
echo "**********50w step2**********"
#time /home/experiment/ywj/singleSMT/data/mgizapp.sh UNPC.50w zh en /home/experiment/ywj/singleSMT/data/training 2 > mgizapp_50w.log 2> mgizapp_50w.md
echo "**********Done 50w step 2 **********"
echo "**********50w step 3**********"
#time /home/experiment/ywj/singleSMT/data/mergeAlignment.sh UNPC.50w zh en /home/experiment/ywj/singleSMT/data/training 2 > merge_50w.log 2> merge_50w.md
echo "**********Done 50w step 3**********"

echo "**********200w step1**********"
#time /home/experiment/ywj/singleSMT/data/prepare.sh UNPC.200w zh en /home/experiment/ywj/singleSMT/data/training 3 > pro_200w.log 2> pro_200w.md
echo "**********Done 200w step 1**********"
echo " **********200w step 2**********"
#time  /home/experiment/ywj/singleSMT/data/mgizapp.sh UNPC.200w zh en /home/experiment/ywj/singleSMT/data/training 3 > mgizapp_200w.log 2> mgizapp_200w.md
echo "**********Done 50w step 2"
echo "**********200w step 3**********"
#time /home/experiment/ywj/singleSMT/data/mergeAlignment.sh UNPC.200w zh en /home/experiment/ywj/singleSMT/data/training 3 > merge_200w.log 2> merge_200w.md
echo "**********Done 200w step 3"

echo "**********300w step1**********"
#time /home/experiment/ywj/singleSMT/data/prepare.sh UNPC.300w zh en /home/experiment/ywj/singleSMT/data/training 4 > pro_300w.log 2> pro_300w.md
echo "**********Done 300w step 1**********"
echo "**********300w step 2**********"
#time /home/experiment/ywj/singleSMT/data/mgizapp.sh UNPC.300w zh en /home/experiment/ywj/singleSMT/data/training 4 > mgizapp_300w.log 2> mgizapp_300w.md
echo "**********Done 300w step 2**********"
echo "**********300w step 3**********"
#time /home/experiment/ywj/singleSMT/data/mergeAlignment.sh UNPC.300w zh en /home/experiment/ywj/singleSMT/data/training 3 > merge_300w.log 2> merge_300w.md
echo "**********Done 300w step 3**********"
