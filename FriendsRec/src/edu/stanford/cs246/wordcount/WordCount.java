package edu.stanford.cs246.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);

		System.exit(res);
	}
	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "WordCount");
		job.setJarByClass(WordCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private final static IntWritable ONE = new IntWritable(1);
		private final static IntWritable ZERO = new IntWritable(0);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String temp[] = value.toString().split("\t");
			String curr = temp[0];
			if(temp.length > 1) {
				String[] list = value.toString().split("\t")[1].split(",");;
				for(String token : list) {
					String record = ZERO.toString() + "," + token;
					context.write(new Text(curr), new Text(record));
				}
				for(String token1 : list) {
					for(String token2 : list) {
						if(!token1.equals(token2)) {
							String record = ONE.toString() + "," + token2;
							context.write(new Text(token1), new Text(record));
						}
					} 
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {   	  
			TreeMap<String, Integer> map = new TreeMap<String, Integer>();
			for(Text value : values) {
				Integer cnt = Integer.parseInt(value.toString().split(",")[0]);
				String friend = value.toString().split(",")[1];
				if(cnt.equals(0)) {
					map.put(friend, 0);
				} else {
					if(!map.containsKey(friend)) {
						map.put(friend, 1);
					} else {
						Integer x = map.get(friend);
						if(!x.equals(0)) {
							x += 1;
							map.put(friend, x);
						}
					}
				}
			}

			Comparator<java.util.Map.Entry<String, Integer>> valueComparator = new Comparator<java.util.Map.Entry<String,Integer>>() {
				@Override
				public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
					if(!o2.getValue().equals(o1.getValue())) {
						return o2.getValue() - o1.getValue();
					} else {
						return Integer.parseInt(o1.getKey()) - Integer.parseInt(o2.getKey());
					}
				}
			};
			List<java.util.Map.Entry<String, Integer>> list = new ArrayList<java.util.Map.Entry<String,Integer>>(map.entrySet());
			Collections.sort(list,valueComparator);    

			String output = "";
			for(int i = 0; i < Math.min(10, list.size()); i++) {
				if(!list.get(i).getValue().equals(0)) {
					output = output + list.get(i).getKey() + ",";
				}
			}
			context.write(new Text(key), new Text(output));
		}
	}
}