import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MayorDetector extends Configured implements Tool{

	public static class MayorMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		
		/*
		Add your code here
		*/
		private IntWritable place = new IntWritable();
		private IntWritable user = new IntWritable();
		
		/**
		 * 
		 * @throws InterruptedException 
		 * @throws IOException 
		*/
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split("|");
			place.set(Integer.parseInt(split[0]));
			user.set(Integer.parseInt(split[1]));
			context.write(place, user);
		}
	}
	
	public static class MayorReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
		/*
		Add your code here
		*/
		private Text word = new Text();

		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			//use HashMap for updating the "uid count"
			Map<Integer,Integer> uidCount = new HashMap<Integer,Integer>();
			for(IntWritable value: values) {
				if(uidCount.containsKey(value.get())){
					uidCount.put(value.get(), uidCount.get(value.get()) + 1);
				}
				else{
					uidCount.put(value.get(), 1);
				}
			}
			//remove those cannot be a mayor
			for(int hashkey : uidCount.keySet()){
				int count = uidCount.get(hashkey);
				if(count < 10){
					uidCount.remove(hashkey);
				}
			}
			//check if the Hashmap is empty
			if (uidCount.isEmpty()){
				//no mayor with count >= 10, write nothing
			}
			else{
				//find the maximum count
				int maxcount = 0;
				for(int hashkey : uidCount.keySet()){
					int count = uidCount.get(hashkey);
					if(count > maxcount){
						maxcount = count;
					}
				}
				//find the keys with the max count
				ArrayList<Integer> uidWithMaxCount = new ArrayList<Integer>();
				for(int hashkey : uidCount.keySet()){
					int count = uidCount.get(hashkey);
					if(count == maxcount){
						uidWithMaxCount.add(hashkey);
					}
				}
				//the size of uidWithMaxCount may not be 1, find the smallest key
				int minUID = uidWithMaxCount.get(0);
				for(int uid : uidWithMaxCount){
					if(uid < minUID){
						minUID = uid;
					}
				}
				word.set(Integer.toString(minUID) + "|" + Integer.toString(maxcount));
				context.write(key, word);
			}
		}
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		
		Job job = new Job(getConf());		
		job.setJarByClass(MayorDetector.class);
		job.setJobName("MayorDetector");
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		/*
		Add your code here
		*/
		job.setMapperClass(MayorMapper.class);
		job.setCombinerClass(MayorReducer.class);
		job.setReducerClass(MayorReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean success = job.waitForCompletion(true);
		
		return success ? 0 : 1;
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int isSuccess = ToolRunner.run(new MayorDetector(), args);
		System.exit(isSuccess);
	}
}
