package SON;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class Main {


    public static void main(String[] args) {
        String fileName = "";

        double[] stepThresholds = {0.01}; // 1% (step1) and 1% (step n>1) of all the baskets
        double minConfidence = 0.5;

        if(args.length>0 && args.length != 3) {
            System.err.println("Usage: <filename> <threshold> <min-confidence>");
            System.exit(-1);
        }

        if(args.length==3) {
            fileName = args[0];
            stepThresholds[0] = Double.parseDouble(args[1]);
            minConfidence = Double.parseDouble(args[2]);
        }
        File inputFile = new File("input/"+fileName);
        log("ANALYSING FILE", inputFile);
        long startTime = System.currentTimeMillis();
        AprioriAlgorithm apriori = new AprioriAlgorithm(stepThresholds, minConfidence, inputFile);
        List<int []> frequentItemsets = apriori.findFrequentItemsets();
        long endTime   = System.currentTimeMillis();
        double totalTime = (endTime - startTime)/ 1000;

        logList("FREQUENT ITEMSETS FOUND:", frequentItemsets);
        log("TIME (in sec): ", totalTime);
        List associations = apriori.findAssociations(frequentItemsets);

    }
    public static void log(String info, Object toLog) {
        System.out.println(info);
        System.out.println(toLog);
    }

    public static void logList(String info, List<int[]> toLog) {
        System.out.println(info);
        for (int[] item: toLog) {
            System.out.print(Arrays.toString(item));
        }
        System.out.println();
    }
}
