package edu.nju.pasalab.mt.util;

import edu.nju.pasalab.mt.wordAlignment.util.HDFSDirInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by YWJ on 2016.12.14.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class CommonFileOperations {

    protected static final Logger LOG =   LoggerFactory.getLogger(CommonFileOperations.class);
    private static FileSystem fs;

    static{
        Configuration conf = new Configuration();
        try {

            //Path hdfsFile = new Path("hdfs://slave001:9000/user/experiment/README.md");
            //conf.addResource(new Path("bin/core-site.xml"));
            //conf.addResource(new Path("bin/hdfs-site.xml"));
            //fs = hdfsFile.getFileSystem(conf);

            fs = FileSystem.get(conf);

        } catch (IOException e) {
            LOG.error("Initializing HDFS operation failed, operation on HDFS will fail!");
        }
    }
    public static void deleteIfExists(String file) throws IOException{
        Configuration conf = new Configuration();

        //Path hdfsFile = new Path(file);
        //conf.addResource(new Path("bin/core-site.xml"));
        //conf.addResource(new Path("bin/hdfs-site.xml"));

        deleteIfExists(file, FileSystem.get(conf));

    }

    public static void deleteHDFSIfExists(String file) throws IOException {
        Configuration conf = new Configuration();
        fs = FileSystem.get(conf);
        if(fs.exists(new Path(file))){
            if(!fs.delete(new Path(file), true)){
                throw new IOException("Cannot delete file : " + file);
            }
        }

    }
    public static void deleteIfExists(String file, FileSystem fs) throws IOException{
        Configuration conf = new Configuration();
        Path hdfsFile = new Path(file);

        //conf.addResource(new Path("bin/core-site.xml"));
        //conf.addResource(new Path("bin/hdfs-site.xml"));

        fs = hdfsFile.getFileSystem(conf);


        if(fs.exists(new Path(file))){
            if(!fs.delete(new Path(file), true)){
                throw new IOException("Cannot delete file : " + file);
            }
        }
    }

    public static InputStream openFileForRead(String file) throws IOException{
        return openFileForRead(file,FileSystem.get(new Configuration()));
    }

    public static InputStream openFileForRead(String file, boolean isOnHDFS) throws IOException{
        if(isOnHDFS){
            return openFileForRead(file,FileSystem.get(new Configuration()));
        }else{
            return new FileInputStream(file);
        }
    }

    public static InputStream openFileForRead(String file, FileSystem fs) throws IOException{
        Configuration conf = new Configuration();
        Path hdfsFile = new Path(file);

        //conf.addResource(new Path("bin/core-site.xml"));
        //conf.addResource(new Path("bin/hdfs-site.xml"));

        fs = hdfsFile.getFileSystem(conf);

        FileStatus fp = fs.getFileStatus(new Path(file));
        if(fp.isDirectory()){
            return new HDFSDirInputStream(file);
        }else{
            FSDataInputStream ins = fs.open(new Path(file));
            return ins;
        }

    }

    public static OutputStream openFileForWrite(String file) throws IOException{
        return openFileForWrite(file,FileSystem.get(new Configuration()));
    }

    public static OutputStream openFileForWrite(String file, boolean isOnHDFS) throws IOException{
        Configuration conf = new Configuration();

        //Path hdfsFile = new Path(file);
        //conf.addResource(new Path("bin/core-site.xml"));
        //conf.addResource(new Path("bin/hdfs-site.xml"));
        //fs = hdfsFile.getFileSystem(conf);


        fs = FileSystem.get(conf);
        if(isOnHDFS) {
            return openFileForWrite(file, FileSystem.get(new Configuration()));
        }
        else
            return new FileOutputStream(file);
    }

    public static OutputStream openFileForWrite(String file, FileSystem fs) throws IOException{
        Configuration conf = new Configuration();
        Path hdfsFile = new Path(file);

        //conf.addResource(new Path("bin/core-site.xml"));
        //conf.addResource(new Path("bin/hdfs-site.xml"));
        //fs = hdfsFile.getFileSystem(conf);

        deleteIfExists(file, fs);
        FSDataOutputStream ins = fs.create(hdfsFile);
        return ins;
    }

    public static void rmr(String file){
        File f = new File(file);
        if(f.isDirectory()){
            String[] files = f.list();
            for(String fn : files){
                rmr(fn);
            }
            f.delete();
        }

        return;
    }

    public static void copyToHDFS(String local, String hdfsFile) throws IOException{
        fs.copyFromLocalFile(false, true, new Path(local), new Path(hdfsFile));
    }

    public static String getHomeDirectory(){
        return fs.getHomeDirectory().toString();
    }

    public static void copyFromHDFS(String hdfsFile, String local) throws IOException{
        rmr(local);
        File f =new File(local);
        f.getAbsoluteFile().getParentFile().mkdirs();
        fs.copyToLocalFile(false,new Path(hdfsFile), new Path(local));
    }

    public static void copyFromHDFSMerge(String hdfsDir, String local) throws IOException{
        rmr(local);
        File f =new File(local);
        f.getAbsoluteFile().getParentFile().mkdirs();
        HDFSDirInputStream inp = new HDFSDirInputStream(hdfsDir);
        FileOutputStream oup = new FileOutputStream(local);
        IOUtils.copyBytes(inp, oup, 65*1024*1024, true);
    }

    public static String[] getAllChildFileHDFS(String file){
        Path p = new Path(file);
        try {
            FileStatus[] fst = fs.listStatus(p);
            int i = 0;
            for(FileStatus f : fst){
                if(!f.isDirectory()){
                    i++;
                }
            }
            String[] ret = new String[i];
            i = 0;
            for(FileStatus f : fst){
                if(!f.isDirectory()){
                    ret[i++] = f.getPath().toString();
                }
            }
            return ret;
        } catch (IOException e) {
            LOG.error("Failed listing dir " + file );
            return null;
        }

    }

    public static String[] getAllChildDirHDFS(String file){
        Path p = new Path(file);
        try {
            FileStatus[] fst = fs.listStatus(p);
            int i = 0;
            for(FileStatus f : fst){
                if(f.isDirectory()){
                    i++;
                }
            }
            String[] ret = new String[i];
            i = 0;
            for(FileStatus f : fst){
                if(f.isDirectory()){
                    ret[i++] = f.getPath().toString();
                }
            }
            return ret;
        } catch (IOException e) {
            LOG.error("Failed listing dir " + file );
            return null;
        }
    }


    /**
     * List all files in a directory that matches a given pattern
     * @param dir     The directory to list
     * @param pattern The pattern which the file's FULL PATH matches, if it is NULL, all file will be included (but no dir will be included)
     * @param subdir  If it is true, then the sub directories will be searched
     * @return  The files in the directory that matches the pattern
     * @throws IOException  Anything bad
     */
    public static String[] listAllFiles(String dir, String pattern, boolean subdir) throws IOException{

        Pattern pat = null;

        if(pattern!=null){
            pat = Pattern.compile(pattern);
        }
        List<String> allFiles = new LinkedList<String>();

        FileStatus[] ft = fs.listStatus(new Path(dir));

        for(FileStatus f : ft){
            if(f.isDirectory() && subdir){
                String[] partial = listAllFiles(f.getPath().toString(),pattern,subdir);
                if(partial == null) continue;
                for(String s : partial) allFiles.add(s);
            }else if(!f.isDirectory()){ // common file
                if(pat == null){
                    allFiles.add(f.getPath().toString());
                }else{
                    if(pat.matcher(f.getPath().toString()).matches()){
                        allFiles.add(f.getPath().toString());
                    }
                }
            }
        }
        String[] str = new String[allFiles.size()];
        int i  = 0;
        for(String s: allFiles) str[i++] = s;
        return str;
    }


    public static String[] readLines(InputStream inp) throws IOException{
        return readLines(inp,"UTF-8");
    }
    public static String[] readLines(InputStream inp, String encoding) throws IOException{
        LinkedList<String> l = new LinkedList<String>();
        BufferedReader bf = new BufferedReader(new InputStreamReader(inp,encoding));
        String line;
        while((line = bf.readLine())!=null){
            l.add(line);
        }
        int i = 0;
        String[] ret = new String[l.size()];
        for(String s : l) ret[i++] = s;
        return ret;
    }

    public static void copyFromDir(String key, String value) {
        // TODO Auto-generated method stub

    }

    /**
     * Get the file size on HDFS
     * IF the path is normal file, the size will be returned,
     * if the path is a directory, the size of all children will be returned.
     * @param inputFile inputfilename
     * @return file num
     * @throws IOException
     */
    public static long getFileSize(String inputFile) throws IOException {
        Path p = new Path(inputFile);
        long count = 0;
        FileStatus status = fs.getFileStatus(p);
        if(!status.isDirectory()){
            count += status.getLen()*status.getBlockSize();
        }else{
            FileStatus[] child = fs.globStatus(new Path(inputFile + "/*"));
            for(FileStatus f : child){
                count += getFileSize(f.getPath().toString());
            }
        }

        return count;
    }

    public static boolean fileExists(String tOutput) {
        try {
            return fs.exists(new Path(tOutput));
        } catch (IOException e) {
            return false;
        }
    }
}
