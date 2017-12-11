package edu.nju.pasalab.mt.wordAlignment.util;

import chaski.utils.PipeInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by YWJ on 2016.12.14.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class HDFSDirInputStream extends PipeInputStream {
    private static final Log LOG = LogFactory.getLog(HDFSDirInputStream.class);
    private FileSystem fs;

    @SuppressWarnings("unchecked")
    protected InputStream getNextStream(String file) {
        try {
            LOG.info("Loading next file " + file);
            return this.fs.open(new Path(file));
        } catch (IOException var3) {
            LOG.warn("openning file " + file + " failed, ignored. ");
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public HDFSDirInputStream(FileSystem fs, String dir) throws IOException {
        this(fs, dir, (Comparator)null);
    }
    @SuppressWarnings("unchecked")
    public HDFSDirInputStream(String dir) throws IOException {
        this(FileSystem.get(new Configuration()), dir, (Comparator)null);
    }


    @SuppressWarnings("unchecked")
    public HDFSDirInputStream(FileSystem fs, String dir, Comparator<String> comp) throws IOException {
        Configuration conf = new Configuration();
        Path hdfsFile = new Path(dir);
        conf.addResource(new Path("bin/core-site.xml"));
        conf.addResource(new Path("bin/hdfs-site.xml"));
        fs = hdfsFile.getFileSystem(conf);

        this.fs = fs;
        Path p = new Path(dir);
        FileStatus fstate = fs.getFileStatus(p);
        if(fstate.isDirectory()) {
            FileStatus[] child = fs.globStatus(new Path(dir + "/*"));
            LinkedList s = new LinkedList();
            HashMap map = new HashMap();
            FileStatus[] it = child;
            int n = child.length;

            for(int pr = 0; pr < n; ++pr) {
                FileStatus c = it[pr];
                if(!c.isDirectory()) {
                    map.put(c.getPath().getName(), c.getPath());
                    s.add(c.getPath().getName());
                }
            }

            if(comp != null) {
                Collections.sort(s, comp);
            } else {
                Collections.sort(s);
            }

            Iterator var15 = s.iterator();

            while(var15.hasNext()) {
                String var13 = (String)var15.next();
                Path var14 = (Path)map.get(var13);
                this.appendFile(var14.toString());
            }
        } else {
            this.appendFile(dir);
        }

    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        HDFSDirInputStream inp = new HDFSDirInputStream(fs, args[0]);
        FileOutputStream ops = new FileOutputStream(args[1]);

        int r;
        while((r = inp.read()) != -1) {
            ops.write(r);
        }

        ops.close();
    }
}
