package edu.nju.pasalab.mt.wordAlignment.util;

import chaski.utils.CommandSheet;
import chaski.utils.ExternalProgramExecuter;
import chaski.utils.ForceCleaningJob;
import edu.nju.pasalab.mt.util.CommonFileOperations;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.List;
import java.util.Map;

/**
 * Created by YWJ on 2016.12.14.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class ExternalUtils {
    //protected static final Log LOG = LogFactory.getLog(ExternalUtils.class);
    protected static final Logger LOG =   LoggerFactory.getLogger(ExternalUtils.class);
    //val logger = LoggerFactory.getLogger(classOf[TrainingSequence])
    static String  curDir = System.getProperty("user.dir");

    /**
     *
     * @param cSheet
     * @param command  An identifier of the command
     * @return
     */
    public static boolean runCommand(CommandSheet cSheet, String command, List<String> message){
        LOG.info("I am running " + command);

        // Copy the files

        Map<String, String> inFile = cSheet.getInitFiles();
        int missing = 0;
        int missingThreshold = cSheet.getMissingFileThreshold();
        for(Map.Entry<String, String> efile : inFile.entrySet()){
            try {
                if(efile.getKey().trim().length()==0 || efile.getValue().trim().length()==0){
                    continue;
                }
                if(cSheet.isInitFileNeedMerging(efile.getKey())){
                    LOG.info("Copying from HDFS " + efile.getKey() + " to " + efile.getValue() + " current directory " + curDir + " (Merging!)");
                    CommonFileOperations.copyFromHDFSMerge(efile.getKey(), efile.getValue());
                }else{
                    LOG.info("Copying from HDFS " + efile.getKey() + " to " + efile.getValue() + " current directory " + curDir + " (Not Merging!)");
                    CommonFileOperations.copyFromHDFS(efile.getKey(), efile.getValue());
                }

                ForceCleaningJob.cleaner.register(efile.getValue());
            } catch (IOException e) {
                LOG.error("Failed on copying " + efile.getKey() + " to " + efile.getValue());
                message.add("Failed on copying " + efile.getKey() + " to " + efile.getValue());
                if(cSheet.isInitFileRequired(efile.getKey())){
                    LOG.error("File required! cannot continue we must exit");
                    return false;
                }else{
                    missing++;
                    if(missing > missingThreshold){
                        LOG.error("Missing files exceeded threshold, we must exit");
                        return false;
                    }
                }
            }
        }


        Map<String, String> outFile = cSheet.getPostFiles();

        for(Map.Entry<String, String> efile : outFile.entrySet()){
            ForceCleaningJob.cleaner.register(efile.getKey());
        }


        for(CommandSheet.Command c : cSheet.getCommands()){
            if(c.isExternal()){
                ExternalProgramExecuter exe = new ExternalProgramExecuter();
                InputStream in = System.in;
                OutputStream out = System.out;
                boolean openedin = false, openedout = false;
                if(c.getRedirStdin()!= null &&c.getRedirStdin().trim().length()>0){
                    try {
                        in = new FileInputStream(c.getRedirStdin().trim());
                        openedin = true;
                    } catch (FileNotFoundException e) {
                        LOG.error("Openning input stream " + c.getRedirStdin().trim() + " failed ");
                        message.add("Openning input stream " + c.getRedirStdin().trim() + " failed ");
                        return false;
                    }
                }
                if(c.getRedirStdout()!= null &&c.getRedirStdout().trim().length()>0){
                    try {
                        out = new FileOutputStream(c.getRedirStdout().trim());
                        openedout = true;
                    } catch (FileNotFoundException e) {
                        LOG.error("Openning output stream " + c.getRedirStdout().trim() + " failed ");
                        message.add("Openning output stream " + c.getRedirStdout().trim() + " failed ");
                    }
                }
                exe.withExecutable(c.getExternal())
                        .withInheritedEnv()
                        .withStdErr(System.err)
                        .withStdIn(in)
                        .withStdOut(out)
                ;

                exe.withParams(c.getArguments());
                for(Map.Entry<String, String> v : c.envEntrySet()){
                    exe.withEnv(v.getKey(),v.getValue());
                }
                LOG.info("Execution started");
                exe.run();
                if(openedin)
                    try {
                        in.close();
                    } catch (IOException e) {

                    }
                if(openedout)
                    try {
                        out.flush();
                        out.close();
                    } catch (IOException e) {
                        LOG.info("Error while closing stdout");
                    }
                LOG.info("Execution completed");
            }else{
                LOG.error("Running jar, NOT IMPLEMENTED");
                message.add("Running jar, NOT IMPLEMENTED");
                return false;
            }
        }

        for(Map.Entry<String, String> efile : outFile.entrySet()){
            try {
                LOG.info("Copying from Local path " + efile.getKey() + " to HDFS: " + efile.getValue() + " current directory " + curDir);
                CommonFileOperations.copyToHDFS(efile.getKey(), efile.getValue());
                ForceCleaningJob.cleaner.register(efile.getKey());
            } catch (IOException e) {
                LOG.error("Failed on copying " + efile.getKey() + " to " + efile.getValue());
                message.add("Failed on copying " + efile.getKey() + " to " + efile.getValue());
                return false;
            }
        }
        return true;
    }


    public static class ReportDaemon extends Thread{

        @SuppressWarnings("unchecked")
        private Mapper.Context context;
        private boolean cancel = false;
        private long interval = 1000L; // every one second, by default

        @SuppressWarnings("unchecked")
        public ReportDaemon(Mapper.Context context){
            this.context = context;
        }

        @SuppressWarnings("unchecked")
        public ReportDaemon(Mapper.Context context, long interval){
            this.context = context;
            this.interval = interval;
            this.setDaemon(true); // Do not block VM quit
        }

        public void cancel(){
            this.cancel = true;
        }

        @Override
        public void run(){


            // Continue report to context
            while(!cancel){
                try{
                    context.progress();
                }catch(Throwable e){

                }
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    continue;
                }
            }
        }

        public void setStatus(String string) {
            context.setStatus(string);
        }

    }

//    public static Map<String,String> getFailedParts(String dir) throws IOException{
//        Map<String,String> ret = new TreeMap<String,String>();
//        InputStream inp = new HDFSDirInputStream(dir);
//        BufferedReader rd = new BufferedReader(new InputStreamReader(inp));
//        String line;
//        String succ = ExternalMapper.success.toString();
//        while((line = rd.readLine())!=null){
//            String[] lp = line.split("\t",2);
//            if(lp.length<2){
//                lp = line.split("\\s+",2);
//            }
//            if(lp.length<2)
//                throw new IOException("Unknown input format");
//            if(lp[1].trim().equalsIgnoreCase(succ)){
//                // OK
//            }else{
//                ret.put(lp[0], lp[1]);
//            }
//        }
//        return ret;
//    }

    public static class ReportDaemonReducer extends Thread{

        @SuppressWarnings("unchecked")
        private org.apache.hadoop.mapreduce.Reducer.Context context;
        private boolean cancel = false;
        private long interval = 5000L; // every 5 seconds, by default

        @SuppressWarnings("unchecked")
        public ReportDaemonReducer(org.apache.hadoop.mapreduce.Reducer.Context context){
            this.context = context;
        }

        @SuppressWarnings("unchecked")
        public ReportDaemonReducer(org.apache.hadoop.mapreduce.Reducer.Context context, long interval){
            this.context = context;
            this.setDaemon(true); // Do not block VM quit
            this.interval = interval;
        }

        public void cancel(){
            this.cancel = true;
        }

        @Override
        public void run(){


            // Continue report to context
            while(!cancel){
                try{
                    context.progress();
                }catch(Throwable e){

                }
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    continue;
                }
            }
        }

        public void setStatus(String string) {
            context.setStatus(string);
        }

    }
}
