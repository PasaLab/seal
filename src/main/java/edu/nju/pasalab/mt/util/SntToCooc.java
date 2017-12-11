package edu.nju.pasalab.mt.util;

/**
 * Created by YWJ on 2016.12.14.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class SntToCooc {
    //protected static final Logger LOG =   LoggerFactory.getLogger(SntToCooc.class);

    //private String exePathLocal;
    private String packageHDFS;
    private String rootDir;
    //private String exeSnt2Plain;
    private String exeMkcls;

    public String getRootDir() {
        return rootDir;
    }
    public void setRootDir(String rootDir) {
        this.rootDir = rootDir;
    }

    public SntToCooc(String packageHDFS, String rootDir) {
        super();
        this.packageHDFS = packageHDFS;
        this.rootDir = rootDir;
    }

    /*public SntToCooc(String exePathLocal, String exeSnt2Plain,  String packageHDFS, String rootDir) {
        super();
        this.exePathLocal = exePathLocal;
        this.packageHDFS = packageHDFS;
        this.rootDir = rootDir;
        this.exeSnt2Plain = exeSnt2Plain;
        this.exeMkcls = exeMkcls;
    }*/


    public static String getHDFSTMPath(String rootDir) { return rootDir + "/dCorpus";}
    public static String getCorpusSrcDict(String rootDir) { return rootDir + "/dict/src.idMap";}
    public static String getCorpusTgtDict(String rootDir) { return rootDir + "/dict/tgt.idMap";}
    public static String getSecondSrcDict(String rootDir) { return rootDir + "/dict/second_Src.idMap"; }
    public static String getSecondTgtDict(String rootDir) { return rootDir + "/dict/second_Tgt.idMap"; }

    //public static String getHDFSSrcVCBPath(String rootDir){return rootDir + "/dict/src/part-r-00000";}
    //public static String getHDFSTgtVCBPath(String rootDir){return rootDir + "/dict/tgt/part-r-00000";}
    public static String getHDFSSrcVCBDir(String rootDir){return rootDir + "/dict/src/";}
    public static String getHDFSTgtVCBDir(String rootDir){return rootDir + "/dict/tgt/";}
    public static String getHDFSSrcVCBClassPath(String rootDir){return rootDir + "/dict/src.classes";}
    public static String getHDFSTgtVCBClassPath(String rootDir){return rootDir + "/dict/tgt.classes";}
    public static String getHDFSCorpusDir(String rootDir){return rootDir + "/snt/" ;}
   // public static String getHDFSSrcCorpus(String rootDir,String entry){return rootDir + "/snt/src/" + entry + ".snt";}
  //  public static String getHDFSTgtCorpus(String rootDir,String entry){return rootDir + "/snt/tar/" + entry + ".snt";}
    public static String getHDFSCoocDir(String rootDir){return rootDir + "/cooc/";}
   // public static String getHDFSSrcCooc(String rootDir,String entry){return rootDir + "/cooc/src/" + entry + ".cooc";}
  //  public static String getHDFSTgtCooc(String rootDir,String entry){return rootDir + "/cooc/tgt/" + entry + ".cooc";}
    public static String getHDFSSrcCorpus(String rootDir,int entry){return rootDir + "/snt/src/" + String.format("%08d", entry) + ".snt";}
    public static String getHDFSTgtCorpus(String rootDir,int entry){return rootDir + "/snt/tar/" + String.format("%08d", entry) + ".snt";}
    public static String getHDFSSrcCoocDir(String rootDir,int entry){return rootDir + "/cooc/src/" + String.format("%08d", entry) + ".cooc";}
    public static String getHDFSTgtCoocDir(String rootDir,int entry){return rootDir + "/cooc/tgt/" + String.format("%08d", entry) + ".cooc";}
    //public static String getHDFSSrcCooc(String rootDir,int entry){return rootDir + "/cooc/src/" + String.format("%08d", entry) + ".cooc/part-r-00000";}
   // public static String getHDFSTgtCooc(String rootDir,int entry){return rootDir + "/cooc/tgt/" + String.format("%08d", entry) + ".cooc/part-r-00000";}
   // public static String getCtrlFile(String rootDir){return rootDir + "/ctrl";}

    public static String getGTGramPath(String rootDir, int n) { return rootDir + "/gt/" + n ;}
    public static String getKNGramPath(String rootDir, int n) { return rootDir + "/kn/" + n ;}
    public static String getMKNGramPath(String rootDir, int n) { return rootDir + "/mkn/" + n ;}
    public static String getGSBGramPath(String rootDir, int n) { return rootDir + "/gsb/" + n ;}
    public static String getLMDictPath(String rootDir) { return rootDir + "/id.map"; }

    public static String getLikelihoodFile(String rootDir, boolean src, int iter) { return String.format("%s/mkcls/%s/it%03d/likelihood", rootDir, src ? "src" : "tgt", iter); }
    public static String getClassMapDir(String rootDir, boolean src, int iter) { return String.format("%s/mkcls/%s/it%03d/classmap", rootDir, src ? "src" : "tgt", iter); }
    public static String getBigramDir(String rootDir, boolean src, int iter) { return String.format("%s/mkcls/%s/it%03d/bigram", rootDir, src ? "src" : "tgt", iter); }
    public static String getClsBigramDir(String rootDir, boolean src, int iter) { return String.format("%s/mkcls/%s/it%03d/classbigram", rootDir, src ? "src" : "tgt", iter); }



}
