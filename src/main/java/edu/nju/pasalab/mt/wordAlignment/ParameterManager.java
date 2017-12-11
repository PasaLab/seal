/*
package edu.nju.pasalab.mt.wordAlignment;


import edu.nju.pasalab.mt.wordAlignment.compo.ReadFromHDFS;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.io.IOException;

*/
/**
 * Created by dong on 2016/1/13.
 *//*

public class ParameterManager {
    private static String fileName="";
    public static JedisPool pool = null;

    public ParameterManager(String name){
        if(fileName.isEmpty()||!fileName.equals(name)) {
            synchronized (fileName) {
                if(fileName.isEmpty()||!fileName.equals(name)) {
                    long startTime = System.currentTimeMillis();
                    initialize(name);
                    long endTime = System.currentTimeMillis();
                    System.out.println("RRRRRRRRRRRR load time: "+(endTime-startTime)/1000.0);
                    fileName = name;
                }
            }
        }
    }

    public static void cleanup(){
        if(pool!=null){
            pool.destroy();
        }
    }

    private void initialize(String name){
        if(pool==null){
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(25);
            config.setMaxIdle(25);
            pool = new JedisPool(config,"localhost");
        }
        Jedis jedis = pool.getResource();
        Pipeline pipeline = jedis.pipelined();
        ReadFromHDFS.writeTranslationTableToRedis(name,pipeline);
        try {
            pipeline.close();
        }catch (IOException ex){
            ex.printStackTrace();
        }finally {
            jedis.close();
        }
    }

}
*/
