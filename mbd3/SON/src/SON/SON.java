package SON;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.ArrayList; 
import java.util.Arrays; 
import java.util.Collections;
import java.util.regex.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;	
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SON {
	
	public static class APMapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			long FILESIZEs = key.get();
			int s = (int) (conf.getDouble("THRESHOLD", 0) * FILESIZEs);
			int SS = conf.getInt("KTUPLE", 2);
			String[] lines = value.toString().split("\\n");
			TreeMap<String, Integer> singletons = new TreeMap<String, Integer>();
			ArrayList<TreeSet<Integer>> transactions = new ArrayList<TreeSet<Integer>>();
			TreeMap<Integer, TreeMap<String, Integer>> freqsets = new TreeMap<Integer, TreeMap<String, Integer>>();
			
			//parse transactions, count singletons
			for(String l : lines){
				String[] items = l.split(" ");
				TreeSet<Integer> transaction = new TreeSet<Integer>();
				for(String i : items){
					transaction.add(Integer.parseInt(i));
					if(singletons.get(i) == null)singletons.put(i, 1);
					else singletons.put(i, singletons.get(i) + 1);
				}
				transactions.add(transaction);
			}
			
			//prune singletons
			for(Iterator<Entry<String, Integer>> it = singletons.entrySet().iterator(); it.hasNext(); ) {
				Entry<String, Integer> e = it.next();
				if(e.getValue() < s){
					it.remove();
				}
			}
			
			freqsets.put(1, singletons);
			
			for(int i=2; i<=SS; i++){
				TreeMap<String, Integer> newfreq = new TreeMap<String, Integer>();
				for(String s1 : freqsets.get(i-1).keySet()){
					int j = i - 2;
					if(i == 2)j = 1;
					//generate k-tuple using k-1 & k-2 tuple
					for(String s2 : freqsets.get(j).keySet()){
						//convert String to Set first
						TreeSet<Integer> newset = new TreeSet<Integer>();
						String news = "";
						String[] items1 = s1.split(",");
						String[] items2 = s2.split(",");
						for(String s3 : items1){
							newset.add(Integer.parseInt(s3));
						}
						for(String s4 : items2){
							newset.add(Integer.parseInt(s4));
						}
						if(newset.size() == i){
							for(Integer e : newset){
								news += Integer.toString(e) + ",";
							}
							newfreq.put(news.substring(0, news.length()-1), 0);
						}
					}
				}
				
				//count k-tuple
				for(String p : newfreq.keySet()){
					String[] items = p.split(",");
					TreeSet<Integer> freqset = new TreeSet<Integer>();
					for(String s1 : items){
						freqset.add(Integer.parseInt(s1));
					}
					for(TreeSet<Integer> b : transactions){
						if(b.containsAll(freqset))newfreq.put(p, newfreq.get(p) + 1);
					}
				}	
				
				//prune k-tuple
				for(Iterator<Entry<String, Integer>> it = newfreq.entrySet().iterator(); it.hasNext(); ) {
					Entry<String, Integer> e = it.next();
					if(e.getValue() < s){
						it.remove();
					}
				}	
				
				if(newfreq.size() == 0)break;
				freqsets.put(i, newfreq);
			}
			
			for(TreeMap<String, Integer> e1 : freqsets.values()){
				for (Entry<String, Integer> e2 : e1.entrySet()){
					Text item = new Text(e2.getKey());
					IntWritable count = new IntWritable(e2.getValue());
					context.write(item, count);
				}
			}
		}
	}

	public static class APReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,	Context context	) throws IOException, InterruptedException {	
			int sum = 0;
			//sum up for debug purpose
			for (IntWritable val : values){
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class APMapper2 extends Mapper<LongWritable, Text, Text, IntWritable>{
		private TreeMap<String, Integer> freqsets = new TreeMap<String, Integer>();
		
		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			FileSystem hdfs = FileSystem.get(conf);
			FileStatus[] status = hdfs.listStatus(new Path(conf.get("PATH"))); 
			//parse candidate itemsets
			for (int i=0;i<status.length;i++){
				String filepath = status[i].getPath().toString();
				if (filepath.matches("(.*)part-(.*)")) {
					FSDataInputStream inStream = hdfs.open(status[i].getPath());
					BufferedReader in = new BufferedReader(new InputStreamReader(inStream, "UTF-8"));
					String line;
					while ((line = in.readLine()) != null){ 
						String values[] = line.split("\t");
						freqsets.put(values[0], 0);
					}
					in.close();
					inStream.close();
				}
			}			
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split("\\n");			
			TreeMap<Integer, Integer> singletons = new TreeMap<Integer, Integer>();
			for(String l : lines){
				//convert String to Set
				String[] items = l.split(" ");
				TreeSet<String> transaction = new TreeSet<String>();
				for(String i : items){
					transaction.add(i);
				}
				
				//count candidate itemsets in all files
				for(String p : freqsets.keySet()){
					String[] freqitems = p.split(",");
					TreeSet<String> freqset = new TreeSet<String>(Arrays.asList(freqitems));
					if(transaction.containsAll(freqset))freqsets.put(p, freqsets.get(p) + 1);
				}
			}		
			
			for (Entry<String, Integer> e : freqsets.entrySet()){
				Text item = new Text(e.getKey());
				IntWritable count = new IntWritable(e.getValue());
				if(e.getValue() > 0)context.write(item, count);
			}
		}
	}

	public static class APReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,	Context context	) throws IOException, InterruptedException {	
			Configuration conf = context.getConfiguration();
			//careful when threshold is not an Integer
			int s = (int) (conf.getDouble("THRESHOLD", 0) * (conf.getInt("FILESIZE", 0) - 1)) + 1;
			int sum = 0;
			for (IntWritable val : values){
				sum += val.get();
			}
			if(sum >= s)context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 6) {
			System.err.println("Usage: SON <in> <out> <threshold> <filesize> <chunksize> <ktuple>");
			System.exit(2);
		}
		FileSystem hdfs = FileSystem.get(conf);
		conf.set("PATH", otherArgs[1] + "/candidate");
		conf.setDouble("THRESHOLD", Double.parseDouble(otherArgs[2]));
		conf.setInt("FILESIZE", Integer.parseInt(otherArgs[3]));
		//set how many lines per file split
		conf.setInt("mapred.line.input.format.linespermap", Integer.parseInt(otherArgs[4]));
		conf.setInt("KTUPLE", Integer.parseInt(otherArgs[5]));

		Job job1 = new Job(conf, "SON step 1");
		job1.setJarByClass(SON.class);
		job1.setMapperClass(APMapper1.class);
		job1.setReducerClass(APReducer1.class);
		job1.setInputFormatClass(MultiLineInputFormat.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1] + "/candidate"));
		job1.waitForCompletion(true);
		
		Job job2 = new Job(conf, "SON step 2");
		job2.setJarByClass(SON.class);
		job2.setMapperClass(APMapper2.class);
		job2.setReducerClass(APReducer2.class);
		job2.setInputFormatClass(MultiLineInputFormat.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1] + "/result"));
		job2.waitForCompletion(true);	

		TreeMap<String, Integer> setcount = new TreeMap<String, Integer>();
		TreeMap<Integer, ArrayList<TreeSet<Integer>>> freqsets = new TreeMap<Integer, ArrayList<TreeSet<Integer>>>(Collections.reverseOrder());
		FileStatus[] status = hdfs.listStatus(new Path(otherArgs[1] + "/result")); 
		
		//parse frequent itemsets
		for (int i=0;i<status.length;i++){
			String filepath = status[i].getPath().toString();
			if (filepath.matches("(.*)part-(.*)")) {
				FSDataInputStream inStream = hdfs.open(status[i].getPath());
				BufferedReader in = new BufferedReader(new InputStreamReader(inStream, "UTF-8"));
				String line;
				while ((line = in.readLine()) != null){ 
					String values[] = line.split("\t");
					String items[] = values[0].split(",");
					setcount.put(values[0], Integer.parseInt(values[1]));
					TreeSet freqset = new TreeSet<Integer>();
					for(String s : items){
						freqset.add(Integer.parseInt(s));
					}
					if(freqsets.get(items.length) == null)freqsets.put(items.length, new ArrayList<TreeSet<Integer>>());
					freqsets.get(items.length).add(freqset);
				}
				in.close();
				inStream.close();
			}
		}	
		
		ArrayList<TreeSet<Integer>> lfreqsets = new ArrayList<TreeSet<Integer>>();

		//count itemsets regard with cardinality
		System.out.println("-----------Itemsets  Count-----------");
		for(Entry<Integer, ArrayList<TreeSet<Integer>>> e : freqsets.entrySet()){
			System.out.println("Size:" + Integer.toString(e.getKey()) + "\tCount:" + e.getValue().size());
			for(TreeSet<Integer> t : e.getValue()){
				lfreqsets.add(t);
			}
		}
		
		//prune singletons
		for(int i=lfreqsets.size()-1; i>=0; i--){
			if(lfreqsets.get(i).size() == 1)lfreqsets.remove(i);
		}
		
		//prune subsets
		for(int i=0; i<lfreqsets.size(); i++){
			for(int j=lfreqsets.size()-1; j>i; j--){
				if(lfreqsets.get(i).containsAll(lfreqsets.get(j)))lfreqsets.remove(j);
			}
		}
		
		System.out.println();
		System.out.println("----------Frequent Itemsets----------");
		System.out.println(StringUtils.rightPad("Itemset", 20) + "Count");			
		for(Entry<String, Integer> e : setcount.entrySet()){
			System.out.println(StringUtils.rightPad(e.getKey(), 20) + e.getValue());	
		}
		
		System.out.println();	
		System.out.println("----------Association Rules----------");	
		System.out.println(StringUtils.rightPad("Rule", 25) + StringUtils.rightPad("Confidence", 25) + StringUtils.rightPad("Interest", 25));	
		for(TreeSet<Integer> s : lfreqsets){
			Integer[] sub = s.toArray(new Integer[s.size()]);
			String freqset = "";
			String from = "";
			for(int i=0; i<sub.length; i++){
				freqset += Integer.toString(sub[i]) + ",";	
			}
			freqset = freqset.substring(0, freqset.length()-1);	
			//extract each element in the set
			for(int i=0; i<sub.length; i++){
				from = "";
				for(int j=0; j<sub.length; j++){
					if(i != j)from += Integer.toString(sub[j]) + ",";
				}
				from = from.substring(0, from.length()-1);	
				double confidence = (double)setcount.get(freqset) / (double)setcount.get(from);
				double interest = confidence - (double)setcount.get(Integer.toString(sub[i])) / Double.parseDouble(otherArgs[3]);
				System.out.println(StringUtils.rightPad("[" + from + "]→" + Integer.toString(sub[i]), 25) + StringUtils.rightPad(Double.toString(confidence), 25) 
					+ StringUtils.rightPad(Double.toString(interest), 25));
			}		
		}
		
		System.exit(0);
	}
}
