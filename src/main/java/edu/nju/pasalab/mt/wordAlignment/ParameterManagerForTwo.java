/*
package edu.nju.pasalab.mt.wordAlignment;

import edu.nju.pasalab.mt.wordAlignment.compo.ReadFromHDFS;
import gnu.trove.map.hash.TObjectDoubleHashMap;

*/
/**
 * Created by dong on 2016/1/16.
 *//*

public class ParameterManagerForTwo {
    private static String iterationPath="";
    private static String positionPath="";

    private static class ParameterContainer{
        private static TObjectDoubleHashMap<String> parameters = new TObjectDoubleHashMap<String>();
        private static TObjectDoubleHashMap<String> positions = new TObjectDoubleHashMap<String>();

        public static void initialize(String iterPath,String posPath){
            //seems no need to synchronize parameters
//            synchronized (parameters) {
                parameters.clear();
                positions.clear();
                ReadFromHDFS.readTranslationTable_trove(iterPath, parameters);
                ReadFromHDFS.readAlignmentTable_trove(posPath,positions);
//            }
        }

        public static boolean isParameterExists(String key){
            return parameters.containsKey(key);
        }

        public static boolean isPositionExists(String key) {return positions.containsKey(key);}

        public static double getParameter(String key){
            return parameters.get(key);
        }

        public static double getPosition(String key) {return positions.get(key);}
    }

    public ParameterManagerForTwo(String iterPath,String posPath){
        if(iterationPath.isEmpty()||!iterationPath.equals(iterPath)) {
            synchronized (iterationPath) {
                if(iterationPath.isEmpty()||!iterationPath.equals(iterPath)) {
                    long t1 = System.currentTimeMillis();
                    ParameterContainer.initialize(iterPath,posPath);
                    long t2 = System.currentTimeMillis();
                    System.out.println("HHHHHH Load time:" + (t2 - t1)/ 1000.0);

                    positionPath = posPath;
                    iterationPath=iterPath;
                }
            }
        }
    }

    public boolean isParameterExists(String key){
        return ParameterContainer.isParameterExists(key);
    }

    public boolean isPositionExists(String key){ return ParameterContainer.isPositionExists(key);}

    public double getParameter(String key){
        return ParameterContainer.getParameter(key);
    }

    public double getPosition(String key) {return ParameterContainer.getPosition(key);}
}
*/
