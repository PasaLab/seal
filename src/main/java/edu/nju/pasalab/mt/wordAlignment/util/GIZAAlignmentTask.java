package edu.nju.pasalab.mt.wordAlignment.util;

import chaski.utils.BinaryToStringCodec;
import chaski.utils.CommandSheet;
import edu.nju.pasalab.mt.util.CommonFileOperations;
import edu.nju.pasalab.mt.util.SntToCooc;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.*;
import java.util.*;

/**
 * Created by YWJ on 2016.6.18.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class GIZAAlignmentTask implements Serializable{
    protected static final Log LOG = LogFactory.getLog(GIZAAlignmentTask.class);

    //////////////////// Static Field and Constructors ///////////////////
    public static String gizabinary = null; // The binary of GIZA on HDFS
    public static String hmmnormbinaray = null; // The binary of HMM normalization program
    public static String d4normbinary = null; // The binary for d-table 4
    public static String symalbin = null;
    public static String symalscript = null;
    public static String giza2bal = null;
    public static String symalmethod = null;
    public static boolean needCopyBinaryFromHDFS = false;

    private static Map<String,String> defaultGIZAParameters = initDefaultParameters(); // Default giza parameters

    private static Map<String,String> initDefaultParameters(){
        TreeMap<String,String> ret = new TreeMap<String,String>();
        ret.put("adbackoff","0");
        ret.put("compactadtable","1");
        ret.put("compactalignmentformat","0");
        ret.put("countcutoff","1e-06");
        ret.put("countcutoffal","1e-05");
        ret.put("countincreasecutoff","1e-07");
        ret.put("countincreasecutoffal","1e-07");
        ret.put("deficientdistortionforemptyword","0");
        ret.put("depm4","76");
        ret.put("depm5","68");
        ret.put("dictionary","");
        ret.put("dopeggingyn","0");
        ret.put("emalignmentdependencies","2");
        ret.put("emalsmooth","0.2");
        ret.put("emprobforempty","0.4");
        ret.put("emsmoothhmm","2");
        ret.put("log","0");
        ret.put("manlexfactor1","0");
        ret.put("manlexfactor2","0");
        ret.put("manlexmaxmultiplicity","20");
        ret.put("maxfertility","6");
        ret.put("maxsentencelength","2500");
        ret.put("mincountincrease","1e-07");
        ret.put("model23smoothfactor","0");
        ret.put("model5smoothfactor","0.1");
        ret.put("nbestalignments","0");

        ret.put("ncpus", "8");
        ret.put("nodumps","0");
        ret.put("nofiledumpsyn","0");
        ret.put("nsmooth","4");
        ret.put("nsmoothgeneral","0");
        ret.put("o","./tmp/norm");
        ret.put("onlyaldumps","0");
        ret.put("p","0");
        ret.put("p0","0.999");
        ret.put("peggedcutoff","0.03");
        ret.put("pegging","0");
        ret.put("probcutoff","1e-07");
        ret.put("probsmooth","1e-07");
        ret.put("readtableprefix","");
        ret.put("s",localSrcVcb);
        ret.put("t",localTgtVcb);
        ret.put("c", localCorpus);
        ret.put("coocurrencefile", localCooc);
        ret.put("tc","");
        ret.put("verbose","0");
        ret.put("verbosesentence","-10");
        return ret;
    }

    public static boolean isNeedCopyBinaryFromHDFS() {
        return needCopyBinaryFromHDFS;
    }

    public static void setNeedCopyBinaryFromHDFS(boolean needCopyBinaryFromHDFS) {
        GIZAAlignmentTask.needCopyBinaryFromHDFS = needCopyBinaryFromHDFS;
    }

    public static String getSymalbin(){ return symalbin;}
    public static void setSymalbin(String symalbin) {
        GIZAAlignmentTask.symalbin =symalbin;
    }
    public static String getSymalscript(){ return symalscript;}
    public static void setSymalscript(String symalscript) {
        GIZAAlignmentTask.symalscript =symalscript;
    }
    public static String getGiza2bal(){ return giza2bal;}
    public static void setGiza2bal(String giza2bal) {
        GIZAAlignmentTask.giza2bal =giza2bal;
    }
    public static String getSymalmethod(){ return symalmethod;}
    public static void setSymalmethod(String symalmethod) {
        GIZAAlignmentTask.symalmethod =symalmethod;
    }

    public static String getGizaBinary() {
        return gizabinary;
    }

    public static void setGizaBinary(String gizabinary) {
        GIZAAlignmentTask.gizabinary = gizabinary;
    }

    public static String getHmmnormBinaray() {
        return hmmnormbinaray;
    }

    public static void setHmmnormBinaray(String hmmnormbinaray) {
        GIZAAlignmentTask.hmmnormbinaray = hmmnormbinaray;
    }

    public static String getD4normBinary() {
        return d4normbinary;
    }

    public static void setD4normBinary(String d4normbinary) {
        GIZAAlignmentTask.d4normbinary = d4normbinary;
    }


    //////////////////// Field and Constructors ///////////////////

    private char startTraining;     // From which stage the training to be resumed?
    private int model1Iterations;   // Number of model 1 iterations to take FOR THE CHILD TASK
    private int hmmIterations;      // Number of HMM iterations to take for the child task
    private int model3Iterations;   // Number of model 3 iterations to take for the child task
    private int model4Iterations;   // Number of model 4 iterations to take for the child task
    private String  generalRoot;
    private String currRoot;        // The root path everthing depends on
    private String prevRoot = null; // The root path of previous step, can be NULL
    private int currStep;
    private boolean isFinal;

    public int getCurrStep() {
        return currStep;
    }

    public void setCurrStep(int currStep) {
        this.currStep = currStep;
    }

    public boolean isFinal() {
        return isFinal;
    }

    public void setFinal(boolean isFinal) {
        this.isFinal = isFinal;
    }

    public String getGeneralRoot() {
        return generalRoot;
    }

    public void setGeneralRoot(String corpusRoot) {
        this.generalRoot = corpusRoot;
    }

    public String getPrevRoot() {
        return prevRoot;
    }

    public void setPrevRoot(String prevRoot) {
        this.prevRoot = prevRoot;
    }

    private Map<String,String> para;// The GIZA parameters to write out


    private boolean verbose;

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public GIZAAlignmentTask(char startTraining, int model1Iterations,
                             int hmmIterations, int model3Iterations, int model4Iterations,
                             String rootPath, Map<String, String> para) {
        super();
        this.startTraining = startTraining;
        this.model1Iterations = model1Iterations;
        this.hmmIterations = hmmIterations;
        this.model3Iterations = model3Iterations;
        this.model4Iterations = model4Iterations;
        this.currRoot = rootPath;
        this.para = new TreeMap<String,String>();
        this.para.putAll(defaultGIZAParameters);
        if(para!=null)
            this.para.putAll(para);
    }

    public GIZAAlignmentTask(){
        super();
        this.para = new TreeMap<String,String>();
        this.para.putAll(defaultGIZAParameters);
    }

    public GIZAAlignmentTask(int cpus){
        super();
        Map<String,String> paras = defaultGIZAParameters;
        if (paras.containsKey("ncpus"))
            paras.put("ncpus", cpus+"");
        this.para = new TreeMap<String,String>();
        this.para.putAll(paras);
        //this.para.put("ncpus",ncpus+"");
    }

    public char getStartTraining() {
        return startTraining;
    }

    public void setStartTraining(char startTraining) {
        this.startTraining = startTraining;
    }

    public int getModel1Iterations() {
        return model1Iterations;
    }

    public void setModel1Iterations(int model1Iterations) {
        this.model1Iterations = model1Iterations;
    }

    public int getHmmIterations() {
        return hmmIterations;
    }

    public void setHmmIterations(int hmmIterations) {
        this.hmmIterations = hmmIterations;
    }

    public int getModel3Iterations() {
        return model3Iterations;
    }

    public void setModel3Iterations(int model3Iterations) {
        this.model3Iterations = model3Iterations;
    }

    public int getModel4Iterations() {
        return model4Iterations;
    }

    public void setModel4Iterations(int model4Iterations) {
        this.model4Iterations = model4Iterations;
    }

    public String getCurrentRoot() {
        return currRoot;
    }

    public void setCurrentRoot(String rootPath) {
        this.currRoot = rootPath;
    }

    public Map<String, String> getPara() {
        return para;
    }

    public void setPara(Map<String, String> para) {
        if(para!=null && para.size() > 0)
            this.para.putAll(para);
    }

    ///////////// Alignment Code ////////////////

    /**
     * The function will write the necessary part file to the directory of control files, and
     * return the directory where the files are written. Note that the directory will be
     * on HDFS and WILL BE OVERWRITTEN WITHOUT NOTICE
     * @param fromPart The task file will be generated from the part ?
     * @param toPart The task file will end by the part ? both of them are inclusive, i.e for(i=fromPart; i<=toPart; i++)
     * @throws IOException
     */
    public ArrayList<String> writeAlignmentTaskFile(int fromPart, int toPart, boolean src2tgt) throws IOException{
        CommonFileOperations.deleteIfExists(currRoot); // Delete the output path if exists
        ArrayList<String> taskStrs = new ArrayList<String>();
        for(int i = fromPart ; i<=toPart; i++){
            String taskString = buildAlignmentTaskString(i,currRoot,src2tgt);
            taskStrs.add(String.format("%08d\t%s",i,taskString));
        }
        return taskStrs;
    }

    protected String buildAlignmentTaskString(int part, String path, boolean src2tgt) throws IOException{
        String outputFileName = String.format("%s/conf/%08d.giza.conf",path,part);
        OutputStream stream = CommonFileOperations.openFileForWrite(outputFileName, true);
        PrintWriter pr = new PrintWriter(new OutputStreamWriter(stream));
        // Also initialize command objects
        CommandSheet cmdsheet = new CommandSheet();
        cmdsheet.addInitFile(outputFileName, part+"_"+localGIZAConf,true);
        cmdsheet.addPostFile(part+"_"+localPerp, this.getPerpFileName(currStep, part));
        if(isFinal){
            cmdsheet.addPostFile(part+"_"+this.getLocalAlignFile(model1Iterations, hmmIterations,model3Iterations
                    , model4Iterations),
                    getAlignFileName("final", part, src2tgt,this.generalRoot));
        }
        LinkedList<CommandSheet.Command> cmds = new LinkedList<CommandSheet.Command>();
        CommandSheet.Command cmd = new CommandSheet.Command();
        // If GIZA binary is ON HDFS, then we need to copy them
        if(needCopyBinaryFromHDFS){
            cmd.setExecutable(part+"_"+localGIZABinary);
            cmdsheet.addInitFile(gizabinary, part+"_"+localGIZABinary,true);
            CommandSheet.Command cmd1 = new CommandSheet.Command();
            cmd1.setExternal(true);
            cmd1.setExecutable("chmod");
            cmd1.setArguments(new String[]{"a+x",part+"_"+localGIZABinary});
            cmds.add(cmd1);
        }else{
            cmd.setExecutable(gizabinary);
        }
        cmd.setArguments(new String[]{part+"_"+localGIZAConf}); // Only one parameter file, which will be generated
        cmd.setExternal(true);
        cmds.add(cmd);
        cmdsheet.setCommands(cmds);

        // After this only thing need to do is set correct files

        // Binary

        // Print out giza config file
        if(para != null){
            para.put("s",part+"_"+localSrcVcb);
            para.put("t",part+"_"+localTgtVcb);
            para.put("c", part+"_"+localCorpus);
            para.put("coocurrencefile", part+"_"+localCooc);
            para.put("o",part+"_tmp/norm");
            for(Map.Entry<String, String> ent : para.entrySet()){
                pr.println(ent.getKey()+" "+ent.getValue());
            }
        }

        // Continue write out parameters
        pr.println("m1 " + model1Iterations);
        pr.println("mh " + hmmIterations);
        pr.println("m3 " + model3Iterations);
        pr.println("m4 " + model4Iterations);

        if(verbose){
            pr.println("t1 1");
            pr.println("th 1");
            pr.println("t3 1");
            pr.println("t4 1");
        }else{
            pr.println("t1 " + model1Iterations);
            pr.println("th " + hmmIterations);
            pr.println("t3 " + model3Iterations);
            pr.println("t4 " + model4Iterations);
        }

        // OK, next step if to set the input / output file
        int restart = 0;

        switch(this.startTraining){
            case '1':
                if(model1Iterations > 0){
                    if(prevRoot==null){ // Nothing to start, init Model 1
                        restart = 0;
                    }else{
                        restart = 1;
                    }
                }else if(hmmIterations > 0){
                    restart = 4;
                }
                //pr.println();
                break;
            case '3':
                if(model1Iterations > 0){
                    restart = 1;
                }else if(hmmIterations > 0){
                    restart = 4;
                }else if(model3Iterations > 0){
                    restart = 9;
                }else if(model4Iterations > 0){
                    restart = 10;
                }
                break;
            case '4':
                if(model1Iterations > 0){
                    restart = 1;
                }else if(hmmIterations > 0){
                    restart = 4;
                }else if(model3Iterations > 0){
                    restart = 9;
                }else if(model4Iterations > 0){
                    restart = 11;
                }
                break;
            case 'H':
                if(model1Iterations > 0){
                    restart = 1;
                }else if(hmmIterations > 0){
                    restart = 6;
                }else if(model3Iterations > 0){
                    restart = 7;
                }
                break;
        }

        // Write restart level
        pr.println("restart " + restart);
        pr.println("dumpcount 1");
        pr.println("countoutputprefix "+part+"_tmp/partial");

        // Based on restart level, set previous models
        switch(restart){
            case 0:
            //case 2:
            //case 3:
            //case 5:
            //case 8:
            default:
                    break;
            case 6:
            case 7:
                pr.println("previoushmm " + part+"_"+localHModel);
                pr.println("previoust " + part+"_"+localTModel);
                pr.println("previousa " + part+"_"+localAModel);
                cmdsheet.addInitDirToFile(getModelPath(part,'t',prevRoot,false), part+"_"+localTModel,true);
                cmdsheet.addInitDirToFile(getModelPath(part,'a',prevRoot,false), part+"_"+localAModel,true);
                String hmodel = getModelPath(part,'h',prevRoot,false);
                cmdsheet.addInitFile(hmodel, part+"_"+localHModel,true);
                cmdsheet.addInitFile(hmodel+".alpha", part+"_"+localHModelA,true);
                cmdsheet.addInitFile(hmodel+".beta", part+"_"+localHModelB,true);
            break;
            case 11:
                pr.println("previousd4 " + part+"_"+local4Model);
                pr.println("previousd42 " + part+"_"+local4Model2);
                String d4model = getModelPath(part,'4',this.prevRoot, false);
                cmdsheet.addInitFile(d4model, part+"_"+local4Model,true);
                cmdsheet.addInitFile(d4model+".b", part+"_"+local4Model2,true);
            case 9:
            case 10:
                pr.println("previousn " + part+"_"+localNModel);
                pr.println("previousd " + part+"_"+localDModel);
                pr.println("previousa " + part+"_"+localAModel);
                cmdsheet.addInitDirToFile(getModelPath(part,'d',prevRoot,false), part+"_"+localDModel,true);
                cmdsheet.addInitDirToFile(getModelPath(part,'n',prevRoot,false), part+"_"+localNModel,true);
                cmdsheet.addInitDirToFile(getModelPath(part,'a',prevRoot,false), part+"_"+localAModel,true);
            case 1:
            case 4:
                pr.println("previoust " + part+"_"+localTModel);
                cmdsheet.addInitDirToFile(getModelPath(part,'t',prevRoot,false), part+"_"+localTModel,true);
                //
        }

        pr.close();

        // We also need to include corpus file and vocabulary and
        if(src2tgt){
            cmdsheet.addInitFile(SntToCooc.getHDFSSrcCorpus(generalRoot, part),part+"_"+localCorpus,true);
            cmdsheet.addInitDirToFile(SntToCooc.getHDFSSrcCoocDir(generalRoot, part), part+"_"+localCooc,true);
            cmdsheet.addInitDirToFile(SntToCooc.getHDFSSrcVCBDir(generalRoot), part+"_"+localSrcVcb,true);
            cmdsheet.addInitDirToFile(SntToCooc.getHDFSTgtVCBDir(generalRoot), part+"_"+localTgtVcb,true);
            cmdsheet.addInitFile(SntToCooc.getHDFSSrcVCBClassPath(generalRoot), part+"_"+localSrcVcbClass,true);
            cmdsheet.addInitFile(SntToCooc.getHDFSTgtVCBClassPath(generalRoot), part+"_"+localTgtVcbClass,true);
        }else{
            cmdsheet.addInitFile(SntToCooc.getHDFSTgtCorpus(generalRoot, part),part+"_"+localCorpus,true);
            cmdsheet.addInitDirToFile(SntToCooc.getHDFSTgtCoocDir(generalRoot, part), part+"_"+localCooc,true);
            cmdsheet.addInitDirToFile(SntToCooc.getHDFSTgtVCBDir(generalRoot), part+"_"+localSrcVcb,true);
            cmdsheet.addInitDirToFile(SntToCooc.getHDFSSrcVCBDir(generalRoot), part+"_"+localTgtVcb,true);
            cmdsheet.addInitFile(SntToCooc.getHDFSTgtVCBClassPath(generalRoot), part+"_"+localSrcVcbClass,true);
            cmdsheet.addInitFile(SntToCooc.getHDFSSrcVCBClassPath(generalRoot), part+"_"+localTgtVcbClass,true);
        }

        // Finally we need to copy file back, depend on the last iteration
        // T-Table: Always needed
        cmdsheet.addPostFile(part+"_"+localTCount, getModelPath(part,'t',currRoot,true));
        // A Table, if HMM or M3/M4
        if(hmmIterations > 0 || model3Iterations > 0 || model4Iterations > 0){
            cmdsheet.addPostFile(part+"_"+localACount, getModelPath(part,'a',currRoot,true));
        }
        // H Table, if ends at HMM
        if(hmmIterations > 0 && model3Iterations == 0 && model4Iterations == 0){
            String hcount = getModelPath(part,'h',currRoot,true);
            cmdsheet.addPostFile(part+"_"+localHCount, hcount);
            cmdsheet.addPostFile(part+"_"+localHCountA, hcount+".alpha");
            cmdsheet.addPostFile(part+"_"+localHCountB, hcount+".beta");
        }
        // D Table, N Table, if model 3 ever started,
        if(model3Iterations > 0 || model4Iterations > 0 ){
            cmdsheet.addPostFile(part+"_"+localNCount, getModelPath(part,'n',currRoot,true));
            cmdsheet.addPostFile(part+"_"+localDCount, getModelPath(part,'d',currRoot,true));
        }

        if(model4Iterations > 0){
            String d4count = getModelPath(part,'4',currRoot,true);
            cmdsheet.addPostFile(part+"_"+ local4Count, d4count);
            cmdsheet.addPostFile(part+"_"+ local4Count2, d4count + ".b");
        }

        BinaryToStringCodec codec = new BinaryToStringCodec(false);


        return codec.encodeObject(cmdsheet);
        // considered done, next step we need to TEST IT
    }

    public static ArrayList<String> buildMerging(String generalRoot, String mergeOutput, int dataSliceNum) throws IOException{
        int parts = dataSliceNum;
        String symbalOnHDFS = generalRoot + "/scripts/symal.sh";
        String symbalLocal= "tmp/symal.sh";
        CommonFileOperations.copyToHDFS(symalscript, symbalOnHDFS);
        String srcAlignLocal = "tmp/s2t.align";
        String tgtAlignLocal = "tmp/t2s.align";
        String localOutput = "tmp/merged.align";
        String taskDir = getMergingTaskDir(generalRoot);
        String resultDir = getMergingResultDir(generalRoot);
        CommonFileOperations.deleteIfExists(taskDir);
        CommonFileOperations.deleteIfExists(resultDir);
        symalmethod = symalmethod.toLowerCase();
        String syma = "";
        if(symalmethod.indexOf("union")>=0){
            syma = "union";
        }
        if(symalmethod.indexOf("intersect")>=0){
            syma = "intersect";
        }
        if(symalmethod.indexOf("grow")>=0){
            syma = "grow";
        }
        if(symalmethod.indexOf("srctotgt")>=0){
            syma = "srctotgt";
        }
        if(symalmethod.indexOf("tgt2src")>=0){
            syma = "tgt2src";
        }
        syma = "-alignment="+syma;
        String symd = symalmethod.indexOf("diag")>=0 ? "-diagonal=yes" : "-diagonal=no";
        String symf = symalmethod.indexOf("final")>=0 ? "-final=yes" : "-final=no";
        String symb = symalmethod.indexOf("final-and")>=0 ? "-both=yes" : "-both=no";
        BinaryToStringCodec codec = new BinaryToStringCodec(false);
        ArrayList<String> taskStrs = new ArrayList<String>();
        for(int i = 1 ; i<=parts ; i++){
            String srcAlign =  getAlignFileName("final",i,true,generalRoot);
            String tgtAlign =  getAlignFileName("final",i,false,generalRoot);

            String outputHDFS = String.format("%s/%08d.merged",mergeOutput,i);

            String taskString = null;
            CommandSheet cmds = new CommandSheet();
            CommandSheet.Command cmd = new CommandSheet.Command();
            cmds.addInitFile(symbalOnHDFS, symbalLocal,true);
            cmds.addInitFile(srcAlign, srcAlignLocal,true);
            cmds.addInitFile(tgtAlign, tgtAlignLocal,true);
            cmd.setExternal(true);
            cmd.setExecutable("bash");
            cmd.setArguments(new String[]{symalscript,localOutput,giza2bal,symalbin,
                    srcAlignLocal,tgtAlignLocal,syma,symd,symf,symb});
            cmds.addPostFile(localOutput, outputHDFS);
            List<CommandSheet.Command> clist = new LinkedList<CommandSheet.Command>();
            clist.add(cmd);
            cmds.setCommands(clist);
            taskString = codec.encodeObject(cmds);
            taskStrs.add(String.format("%08d\t%s",i,taskString));
        }
        return taskStrs;
    }

    public static String getMergingTaskDir(String generalRoot){return generalRoot+"/ctrl-merge/";};
    public static String getMergingResultDir(String generalRoot){return generalRoot+"/result-merge/";};

    ///////////// Utility and definitions ////////////////

    /**
     * Get the path of the model, the model character can be 't', 'a', 'd', 'n', 'h', '4'
     */
    public static String getModelPath(int part, char model, String root, boolean isCount){
        if(!isCount){
            return String.format("%s/model/%c.model", root,model);
        }else{
            return String.format("%s/count/%c/%08d.count",root,model,part);
        }
    }

    public static String getModelDir(String root, char model, boolean isCount){
        if(!isCount){
            return String.format("%s/model/%c.model", root,model);
        }else{
            return String.format("%s/count/%c",root,model);
        }
    }

    public String getPerpFileName(int step,int part){
        return String.format("%s/training/perp/step%03d-part%08d.perp",this.generalRoot,step,part);
    }

    public static String getAlignFileName(String step,int part,boolean ist2s, String generalRoot){
        if(ist2s)
            return String.format("%s/training/S2T/Align/step.%s.part%08d.align",generalRoot,step,part);
        else
            return String.format("%s/training/T2S/Align/step.%s.part%08d.align",generalRoot,step,part);
    }

    public String getLocalAlignFile(int model1Iterations, int hmmIterations, int model3Iterations, int model4Iterations){
        if(model1Iterations > 0 && hmmIterations == 0 && model3Iterations == 0 && model4Iterations ==0){
            return "tmp/norm.A1."+model1Iterations+".part0";
        }
        if(hmmIterations > 0 && model3Iterations == 0 && model4Iterations ==0){
            return "tmp/norm.Ahmm."+hmmIterations+".part0";
        }

        return "tmp/norm.A3.final.part0";
    }

    public static String getTempDir(String root){return root+"/temp";};
    public static String getATableTemp(String root){return getTempDir(root)+"/atemp";};
    public static String getDTableTemp(String root){return getTempDir(root)+"/dtemp";};

    public static final String localGIZABinary = "tmp/mgiza";
    public static final String localGIZAConf = "tmp/mgiza.conf";
    public static final String localTModel = "tmp/t.model";
    public static final String localHModel = "tmp/h.model";
    public static final String localHModelA = "tmp/h.model.alpha";
    public static final String localHModelB = "tmp/h.model.beta";
    public static final String localAModel = "tmp/a.model";
    public static final String localDModel = "tmp/d.model";
    public static final String localNModel = "tmp/n.model";
    public static final String local4Model = "tmp/d4.model";
    public static final String local4Model2 = "tmp/D4.model";
    public static final String localTCount = "tmp/partial.t.count";
    public static final String localHCount = "tmp/partial.h.count";
    public static final String localHCountA = "tmp/partial.h.count.alpha";
    public static final String localHCountB = "tmp/partial.h.count.beta";
    public static final String localACount = "tmp/partial.a.count";
    public static final String local4Count = "tmp/partial.d4.count";
    public static final String localDCount = "tmp/partial.d.count";
    public static final String localNCount = "tmp/partial.n.count";
    public static final String local4Count2 = "tmp/partial.d4.count.b";
    public static final String localCorpus = "tmp/corpus.snt";
    public static final String localSrcVcb = "tmp/src.vcb";
    public static final String localTgtVcb = "tmp/tgt.vcb";

    public static final String localSrcVcbClass = "tmp/src.vcb.classes";
    public static final String localTgtVcbClass = "tmp/tgt.vcb.classes";
    public static final String localCooc = "tmp/corpus.cooc";
    public static final String localPerp = "tmp/norm.perp";
}
