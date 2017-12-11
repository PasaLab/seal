****************************************************************************

                                  Seal
      Training Large Scale Statistical Machine Translation Models on Spark

                    https://github.com/PasaLab/seal

                         Copyright (C) 2017 by
                                PASA NJU
          Rong Gu, Min Chen, Wenjia Yang, Chunfeng Yuan, Yihua Huang
                Nanjing University, Nanjing, China 210093

****************************************************************************


                          TABLE OF CONTENTS

1. Introduction

2. Environment Preparation

3. Compile Seal
   3.1. Download
   3.2. Compile and Package
   
4. How to Use Seal
   4.1. Word Alignment Model
      4.1.1. Parameters Setting
      4.1.2. Parallel Training
   4.2. Translation Model
      4.2.1. Parameters Setting
      4.2.2. Parallel Training
   4.3. Language Model
      4.3.1. Parameters Setting
      4.3.2. Parallel Training

5. Training corpus

****************************************************************************


1. Introduction


  Seal is an end-to-end, efficient and scalable statistical machine translation 
  training toolkit based on the widely-used distributed data-parallel platform 
  Spark, which includes the parallel training of word alignment model, translation 
  models and language model.

  For word alignment model, Seal implements the parallel training of preprocessing,
  word alignment in both directions and merging alignment results.
  
  For translation model, Seal implements the parallel training of the syntactic 
  translation model. Besides, three probability smoothing methods are applied to 
  the model parameters.

  For language model, Seal implements the parallel construction of N-Gram language
  model. And in order to avoid getting zero probability, four kinds of probability
  smoothing methods are also applied to the language model. The default smoothing 
  method of Seal is MKN.

  
2. Environment Preparation


  On Linux environments(Centos 7):

  We use Spark as the data-parallel platform, and all softwares we needed are 
  listed here:
  
                     ******************************************
					 
							Configuration	  Property
							
							JDK Version	       1.8.0
							Spark Version	   2.1.0
							Hadoop Version	   2.7.3
							Moses Version	    2.0
							MGIZA++ Version    0.7.0
							
					******************************************

					
3. Compile Seal


  3.1. Download

  
  You can find and download document, source code of Seal at:

               https://github.com/PasaLab/seal
  

  3.2. Compiling and Packaging

  
  On Linux environments(Centos 7):

  + System requirements:

    - we use Maven to compile and package the project.

  + Install the local jar: chaski-0.0-latest.jar and jpfm-0.1-latest.jar,
    and add them to the pom.xml file, for example:
  
    $ mvn install:install-file -Dfile=chaski-0.0-latest.jar -DgroupId=edu.nju.pasalab 
	      -DartifactId=Chaski -Dversion=1.0 -Dpackaging=jar

	<dependency>
	  <groupId>edu.nju.pasalab</groupId>
	  <artifactId>chaski</artifactId>
	  <version>1.0</version>
	</dependency>
  
  + Compiling:

    $ mvn clean
    $ mvn compile
	
  + Packaging:

    $ mvn clean
    $ mvn -DskipTests package assembly:single


4. How to Use Seal

  
  Seal includes the the parallel training of word alignment model, translation 
  model and language model. We use the configuration file to set the training 
  parameters. You can modify them in the /bin/training.conf file.
  
  
  4.1. Word Alignment Model
  
  
  For word alignment model, Seal implements the parallel training of preprocessing,
  word alignment in both directions and merging alignment results. Before 
  preprocessing, data cleaning is necessary, such as Half-width conversion, tokenizer, 
  case conversion, etc..Please refer to:
  
  http://cwmt2016.xjipc.cas.cn/webpub/resource/10020/Image/CWMT2016_Proceedings.pdf 
  
  
  4.1.1. Parameters Setting
  
	  wam {
		  source = "local source file"          # source language file
		  target = "local target file"          # target language file
		  alignedRoot = "hdfs root"             # work dir 
		  redo = true
		  iterations = 1                        # the number of cluster iteration
		  nCPUs = 2                             # the number of CPU used by MGIZA++
		  memoryLimit = 64                      # the size of block size(MB)
		  numCls = 50                           # the number of clusters
		  maxSentLen = 100  
		  totalReducer = 10
		  splitNum = 1                          # number of blocks
		  trainSeq ="1*H*3*"                    # model training sequence
		  verbose = false
		  overwrite = true
		  nocopy = false

		  # mgiza++ execution file
		  d4norm  = "d4norm"
		  hmmnorm = "hmmnorm"
		  giza = "mgiza"
		  symalbin = "symal"
		  symalmethod = "grow-diag-final-and"
		  symalscript = "symal.sh"
		  giza2bal = "giza2bal.pl"

		  filterlog = "filter-log"
		  srcClass = ""
		  tgtClass = ""
		  encoding = "UTF-8"
		}
  
  
  4.1.2. Parallel Training
  
  - Word cluster:
  
  $ spark-submit spark-submit --master spark://slave201:7077 \
                 --class edu.nju.pasalab.mt.wordAlignment.prodcls.ProduceClass \
                 --name getCorpusSyntaxInfo \
				 --driver-memory 50G \
				 --executor-memory 24G \
				 --total-executor-cores 192 \
				 --executor-cores 8 \
				 /path/parallel-smt-3.0-SNAP-jar-with-dependencies.jar \ 
				 /path/configuration/test.conf 
  
  
  - Split corpus:
  
  $ spark-submit spark-submit --master spark://slave201:7077 \
                 --class edu.nju.pasalab.mt.wordAlignment.wordprep. wordAlignPrep \
                 --name getCorpusSyntaxInfo \
				 --driver-memory 50G \
				 --executor-memory 24G \
				 --total-executor-cores 192 \
				 --executor-cores 8 \
				 /path/parallel-smt-3.0-SNAP-jar-with-dependencies.jar \ 
				 /path/configuration/test.conf 
		
		
  - Word alignment:
  
  $ spark-submit spark-submit --master spark://slave201:7077 \
                 --edu.nju.pasalab.mt.wordAlignment.usemgiza.mainTraining \
                 --name getCorpusSyntaxInfo \
				 --driver-memory 50G \
				 --executor-memory 24G \
				 --total-executor-cores 192 \
				 --executor-cores 8 \
				 /path/parallel-smt-3.0-SNAP-jar-with-dependencies.jar \ 
				 /path/configuration/test.conf 

				 
  4.2. Translation Model
  
  
  Seal implements the parallel training of syntactic translation model, and three kinds of 
  probabilistic smoothing methods are applied, including Good Turing (GT), Kneser-Ney (KN) 
  and Modified Kneser-Ney (MKN).
  
  
  4.2.1. Parameters Setting
  
	  mt {
		  inputDir = "data/LDC.1k"                  # HDFS path of the parallel corpus with word alignment
		  rootDir = "data/phrase"                   # work dir
		  optType = ${ExtractType.Rule}           # the type of translation model
		  
		  # the parameters of extracting translation units
		  maxc = 5
		  maxe = 5
		  maxSpan = 10
		  maxInitPhraseLength = 10
		  maxSlots = 2
		  minCWords = 1
		  minEWords = 1

		  maxCSymbols = 5
		  maxESymbols = 5
		  minWordsInSlot = 1

		  allowConsecutive = false
		  allowAllUnaligned = false
		  c2eWord = "WordTranslationC2E"
		  e2cWord = "WordTranslationE2C"

		  //withNullOption this parameter can be Set true if need
		  withNullOption= false
		  allowBoundaryUnaligned = true
		  cutoff = 0
		  direction = ${Direction.C2E}

		  # smoothing parameters
		  smoothedWeight = 0.2
		  allowGoodTuring = false
		  allowKN = false
		  allowMKN = false
		  allowSmoothing = false

		  #syntax based
		  withSyntaxInfo = True                      # parallel corpus contains the syntax inforamtion or not
		  depType = ${DepType.DepString}             # select the type of eatracted rules
		  posType = ${POSType.AllPossible}
		  constrainedType = ${ConstrainedType.WellFormed}
		  syntax = ${SyntaxType.S2D}                 # just support S2D
		  keepFloatingPOS = false
	}
  
  For syntactic translation model, parallel corpus must contain the syntax information 
  and we need to set "withSyntaxInfo = True", syntax = ${SyntaxType.S2D}
  
  
  4.2.2. Parallel Training
  
  $ spark-submit spark-submit --master spark://slave201:7077 \
                 --class edu.nju.pasalab.mt.extraction.spark.exec.MainExtraction \
                 --name getCorpusSyntaxInfo \
				 --driver-memory 50G \
				 --executor-memory 24G \
				 --total-executor-cores 192 \
				 --executor-cores 8 \
				 /path/parallel-smt-3.0-SNAP-jar-with-dependencies.jar \ 
				 /path/configuration/test.conf 
				 
	in which:
	
	--class:
	    The main class of the Translation Model.

	/path/parallel-smt-3.0-SNAP-jar-with-dependencies.jar: 
	    The path of the project jar.
		
	/path/configuration/test.conf:
	    The path of the configuration file.
   
  
  4.3. Language Model
  
  
  Seal implements the parallel construction of N-Gram language model, and four 
  kinds of probabilistic smoothing methods are applied, including Good Turing (GT),
  Kneser-Ney (KN), Modified Kneser-Ney (MKN) and Google StupidbackOff(GSB). 
  The default smoothing strategy provided by Seal is MKN.
  
  
  4.3.1. Parameters Setting
  
	  lm {
		  lmRootDir = "data/lm"                  # work dir
		  lmInputFileName = "data/test.txt"      # training corpus
		  N = 5                                  # N - gram
		  splitDataRation = 0.01 
		  sampleNum = 10
		  expectedErrorRate = 0.01
		  offset = 100
		  
		  # smoothing method
		  GT = false                              # Good Turing
		  GSB = false                             # StupidBackOff
		  KN = false                              # Kneser-Ney
		  MKN = true                              # Modified Kneser-Ney
		}
  
  
  4.3.2. Parallel Training
  
  $ spark-submit --master spark://slave201:7077 \
                 --class edu.nju.pasalab.mt.LanguageModel.spark.exec.lmMain \
                 --name Language Model Training \
                 --driver-memory 50G \
                 --executor-memory 20G \
                 --total-executor-cores 192 \
                 --executor-cores 8 \
                 /path/parallel-smt-3.0-SNAP-jar-with-dependencies.jar \ 
                 /path/configuration/test.conf
  
    in which:
	
	--class:
	    The main class of the Language Model.

	/path/parallel-smt-3.0-SNAP-jar-with-dependencies.jar: 
	    The path of the project jar.
		
	/path/configuration/test.conf:
	    The path of the configuration file.
		
		
5. Training corpus


   There are two available Chinese-English bilingual parallel corpus, one of 
   which is released by LDC1(Linguistic Data Consortium) with over 8 million
   sentence pairs and the other is released by UNPC (United Nations Parallel 
   Corpus) with over 15 million sentence pairs.
   
   You can find and download it here:
   
   Linguistic Data Consortium: https://www.ldc.upenn.edu/

   M. Ziemski, M. Junczys-Dowmunt, B. Pouliquen, The united nations parallel 
   corpus v1. 0, in: Proceedings of the Tenth International Conference
   820 on Language Resources and Evaluation LREC, 2016, pp. 23¨C28

