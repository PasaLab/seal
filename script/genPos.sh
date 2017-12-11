#!/bin/bash
list_allDir() {
    for file2 in `ls -a $1`
    do
	if [ x"$file2" != x"." -a x$"file2" != x".." ]; then
           if [ -f "$1/$file2" ]; then
              echo "****************************start process $file2******************************"
              echo "save to $2/pos-$file2"
              echo "save to $3/head-$file2"
              java -cp parser.jar edu.nju.nlp.mt.parser "$1/$file2" "$2/pos-$file2" "$3/head-$file2"
              echo "******************************Done******************************************"
	      #cat "$1/$file2" >> $2
	   fi
	fi
    done
}
list_allDir /home/ywj/wordsplit/tmp /home/ywj/wordsplit/postag /home/ywj/wordsplit/headIndex
