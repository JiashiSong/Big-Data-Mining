package SON;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


public class simpleRandom {
    
    private int supportThreshold;
    private double[] stepThresholds;
    private final double minConfidence;
    private int numberOfItems;
    private int numberOfBaskets;
    private int kItemsets;

    private List<int[]> dataset;
    private List<int[]> kFrequentItemsets, totalFrequentItemsets, candidateItemsets;
    private Map<Integer, Integer> frequentItemsetSupports;
    
    public simpleRandom(double[] s, double minConfidence, File fileToAnalyse) {

        // set attributes
        this.numberOfBaskets = 0;
        this.numberOfItems = 0;
        this.candidateItemsets = new ArrayList<>();
        this.kFrequentItemsets = new ArrayList<>();
        this.totalFrequentItemsets = new ArrayList<>();
        this.frequentItemsetSupports = new HashMap<>();

        // prepare the file
        this.prepareFile(fileToAnalyse);

        // set the threshold
        this.supportThreshold = (int) (numberOfBaskets*s[0]);
        this.stepThresholds = s;
        this.minConfidence = minConfidence;

        // print out information
        log("Items: ", this.numberOfItems);
        log("Baskets: ", this.numberOfBaskets);
        log("Threshold: ", this.supportThreshold);
        log("Min Confidence: ", this.minConfidence);
        // compute the first step: frequent singletons
        this.generateSingletons();
        this.computeFrequentSingletons();

        logList("Frequent singletons ("+this.kFrequentItemsets.size()+"): ",this.kFrequentItemsets);

    }
    private void prepareFile(File fileToAnalyse) {

        BufferedReader br = null;
        dataset = new ArrayList();

        try {
            br = new BufferedReader(new FileReader(fileToAnalyse));

            while(br.ready()) {

                // split the line to get the basket, and increase the counter
                int[] basket = Arrays
                        .asList(br.readLine().split("\\s"))
                        .stream()
                        .mapToInt(Integer::parseInt)
                        .toArray();

                numberOfBaskets++;
                dataset.add(basket);

                for(int item: basket) {
                    // set the number of items
                    if(numberOfItems<item+1) numberOfItems = item+1;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // close the buffer
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void generateSingletons() {

        kItemsets = 1;
        for(int i=0; i<numberOfItems; i++) {
            int[] singleton = {i};
            candidateItemsets.add(singleton);
        }
    }
    private void computeFrequentSingletons() {
        
        int[] support = new int[numberOfItems];

        Iterator<int[]> basketsIterator = dataset.iterator();

        while(basketsIterator.hasNext()) {

            int[] basket = basketsIterator.next();

            for(int item: basket) {
                support[item]++;
            }
        }

        // iterate over the candidates to filter them
        filterCandidates(support);
    }

    public List<int[]> findFrequentItemsets() {

        // supports vector (used to count the frequences)
        int[] support;

        for(this.kItemsets=2; this.kFrequentItemsets.size() != 0; this.kItemsets++) {

            // set a new threshold id (if the user has specified more than one)
            if(this.stepThresholds.length > 1) {
                double t = stepThresholds[1];
                this.supportThreshold = (int) (this.numberOfBaskets*t);
            }

            // generate the set of candidates
            this.candidateItemsets = aprioriGen(this.kFrequentItemsets);

            // instantiate the supports vector
            support = new int[this.candidateItemsets.size()];

            // iterate over the dataset
            Iterator<int[]> basketsIterator = this.dataset.iterator();

            while(basketsIterator.hasNext()) {

                int[] basket = basketsIterator.next();

                // create a boolean array where the basket[item] = true
                boolean[] basketItems = Combination.convertToBoolean(basket, numberOfItems);

                // check if any of the candidates is contained in the basket
                for(int c=0; c<candidateItemsets.size(); c++) {
                    int[] candidateItemset = candidateItemsets.get(c);
                    boolean contained = true;

                    for(int item: candidateItemset){
                        if(!basketItems[item]) {
                            contained = false;
                            break;
                        }
                    }

                    // if the itemset is contained in the basket increase its support
                    if(contained) support[c]++;
                }
            }

            totalFrequentItemsets.addAll(this.kFrequentItemsets);
            this.kFrequentItemsets = new ArrayList<>();

            // filter the candidates
            filterCandidates(support);

            logList("Frequent itemsets of size "+kItemsets+" ("+this.kFrequentItemsets.size()+"):" ,this.kFrequentItemsets);
        }
        return totalFrequentItemsets;
    }

    private void filterCandidates(int[] support) {
        // iterate over the candidates to filter them
        for(int candidate=0; candidate<support.length; candidate++) {
            // if the support is greater than the threshold
            if (support[candidate] >= this.supportThreshold) {
                // put the itemset int the set of frequent itemsets (the put will overwrite existing values)
                int[] c = this.candidateItemsets.get(candidate);
                this.kFrequentItemsets.add(c);
                this.frequentItemsetSupports.put(Arrays.hashCode(c), support[candidate]);
            }
        }
    }
    private List<int[]> aprioriGen(List<int[]> oldFrequentItemsets) {

        // combine and prune
        return Combination.combine(oldFrequentItemsets);
    }
    public List findAssociations(List<int[]> frequentItemsets) {
        for (int[] itemset: frequentItemsets) {

            if(itemset.length >= 2) {
                List<int[]> singleConsequents = Combination.combinations(itemset, 1);
                int itemsetSupport = this.frequentItemsetSupports.get(Arrays.hashCode(itemset));
                int diffSupport;
                double confidence;
                List<int[]> toRemove = new ArrayList<>();
                for(int[] consequent: singleConsequents) {
                    int[] diff = Combination.setDifference(consequent, itemset);

                    diffSupport = this.frequentItemsetSupports.get(Arrays.hashCode(diff));

                    confidence = new Double(itemsetSupport) / new Double(diffSupport);

                    if(confidence >= this.minConfidence) {
                        System.out.println("RULE: "+Arrays.toString(diff)+" => "+Arrays.toString(consequent)+" with conf = "+confidence+" and support= "+itemsetSupport);
                    } else {
                        // delete the rule from the set
                        toRemove.add(consequent);
                    }
                }
                // remove items
                for(int[] torem: toRemove) {
                    singleConsequents.remove(torem);
                }
                this.findAssociations(itemset, singleConsequents);
            }
        }
        return null;
    }

    private void findAssociations(int[] itemset, List<int[]> subset) {

        int itemsetSupport = this.frequentItemsetSupports.get(Arrays.hashCode(itemset));
        int diffSupport;
        double confidence;
        List<int[]> toRemove = new ArrayList<>();

        if(subset.size() > 0 && itemset.length > subset.get(0).length+1) {

            List<int[]> consequents = Combination.combine(subset);
            for(int[] consequent: consequents) {
                int[] diff = Combination.setDifference(consequent, itemset);

                diffSupport = this.frequentItemsetSupports.get(Arrays.hashCode(diff));

                confidence = new Double(itemsetSupport) / new Double(diffSupport);

                if(confidence >= this.minConfidence) {
                    System.out.println("RULE: "+Arrays.toString(diff)+" => "+Arrays.toString(consequent)+" with conf = "+confidence+" and support= "+itemsetSupport);
                } else {
                    
                    toRemove.add(consequent);
                }
            }
            
            for(int[] torem: toRemove) {
                consequents.remove(torem);
            }
            findAssociations(itemset, consequents);
        }
    }
    private static void log(String info, Object toLog) {
        System.out.println(info);
        System.out.println(toLog);
    }

    private static void logList(String info, List<int[]> toLog) {
        System.out.println(info);
        for (int[] item: toLog) {
            System.out.print(Arrays.toString(item));
        }
        System.out.println();

    }
}
