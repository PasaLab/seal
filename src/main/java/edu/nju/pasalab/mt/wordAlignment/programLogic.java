package edu.nju.pasalab.mt.wordAlignment;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;

/**
 * Created by BranY on 2016.6.6.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class programLogic {
    /////////////////////////////
    // Definition of error codes
    //
    // File not found
    public static final int FILE_NOT_FOUND      = 0x80001020;
    public static final int FILE_ALREADY_EXISTS = 0x80001021;
    public static final FileSystem fs = null;

    //////////////////
    private static String lastErrorMessage = null;
    private static int lastErrorCode = 0;

    /**
     * Non-fatal test if file exists,
     */
    public static boolean testFileExist(String file, String message, Log logger){
        File fs = new File(file);
        if(fs.exists()){
            return true;
        }else{
            reportNonFatal(FILE_NOT_FOUND, message, logger);
            return false;
        }
    }

    public static boolean testFileNotExist(String file, String message, Log logger){
        File f = new File(file);
        if(!f.exists()){
            return true;
        }else{
            reportNonFatal(FILE_ALREADY_EXISTS,message,logger);
            return false;
        }
    }

    /**
     * Fatal test if file exists
     */
    public static void testFileExistOrDie(String file, String message, Class<? extends Exception> exp, Log logger) throws Exception{
        if(!testFileExist(file,message,logger)){
            reportFatal(FILE_NOT_FOUND,message,logger,exp);
        }
    }
    public static void testFileNotExistOrDie(String file, String message, Class<? extends Exception> exp, Log logger) throws Exception{
        if(!testFileNotExist(file,message,logger)){
            reportFatal(FILE_ALREADY_EXISTS,message,logger,exp);
        }
    }

    /**
     * Non-fatal test if file exists,
     */
    public static boolean testFileExistOnHDFS(String file, String message, Log logger){
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            if(fs.exists(new Path(file))){
                return true;
            }else{
                reportNonFatal(FILE_NOT_FOUND,message,logger);
                return false;
            }
        } catch (IOException e) {
            reportNonFatal(FILE_NOT_FOUND,message,logger);
            return false;
        }
    }

    public static boolean testFileNotExistOnHDFS(String file, String message, Log logger){
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            if(!fs.exists(new Path(file))){
                return true;
            }else{
                reportNonFatal(FILE_ALREADY_EXISTS,message,logger);
                return false;
            }
        } catch (IOException e) {
            reportNonFatal(FILE_ALREADY_EXISTS,message + " Initializing file system failed ",logger);
            return false;
        }
    }

    /**
     * Fatal test if file exists
     */
    public static void testFileExistOnHDFSOrDie(String file, String message, Class<? extends Exception> exp, Log logger) throws Exception{
        if(!testFileExistOnHDFS(file,message,logger)){
            reportFatal(FILE_NOT_FOUND, message, logger, exp);
        }
    }

    public static void testFileNotExistOnHDFSOrDie(String file, String message, Class<? extends Exception> exp, Log logger) throws Exception{
        if(!testFileNotExistOnHDFS(file,message,logger)){
            reportFatal(FILE_ALREADY_EXISTS,message,logger,exp);
        }
    }

    /**
     * If it exists on either place
     */
    public static boolean testFileExistOnHDFSorLocal(String file, String message, Log logger){
        return testFileExist(file,message,logger) || testFileExistOnHDFS(file,message,logger);
    }


    public static void testFileExistOnHDFSorLocalOrDie(String file, String message, Class<? extends Exception> exp, Log logger) throws Exception{
        if(!testFileExistOnHDFSorLocal(file,message,logger)){
            reportFatal(FILE_NOT_FOUND,message,logger,exp);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////
    // Utility functions
    //////////////////////////////////////////////////////////////////////////////////////////////




    /// Report non-fatal error
    private static void reportNonFatal(int errorCode, String message, Log logger){
        lastErrorCode = errorCode;
        lastErrorMessage = message;
        if(logger!=null){
            logger.error(message);
        }else{
            System.err.println("[Error]"+message);
        }
    }

    /// Report Fatal error
    private static void reportFatal(int errorCode, String message, Log logger, Class<? extends Exception> exp) throws Exception{
        lastErrorCode = errorCode;
        lastErrorMessage = message;
        if(logger!=null){
            logger.fatal(message);
        }else{
            System.err.println("[Error]"+ message);
        }
        try {
            Constructor<?>[] cons = exp.getConstructors();
            for(Constructor<?> c : cons){
                if(c.getParameterTypes().length == 1 && c.getParameterTypes()[0].equals(String.class)){
                    Object[] obj = new Object[1];
                    obj[0] = message;
                    try{
                        Exception ex = (Exception) c.newInstance(obj);
                        throw ex;
                    }catch(Exception e){
                        // Do nothing
                    }
                }
            }
            Exception ex = exp.newInstance();
            throw ex;
        } catch (InstantiationException e) {
            throw new Exception(message);
        } catch (IllegalAccessException e) {
            throw new Exception(message);
        }
    }
}
