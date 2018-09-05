package bigdata;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/***
 * MapReduce
 * @author root
 *
 */
public class WordCount {
	
		//step1 : map class
		public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

			private Text mapOutputKey = new Text();
			private final static IntWritable mapOutputValue = new IntWritable(1);
			
			@Override
			public void map(LongWritable key, Text value,Mapper<LongWritable, Text, Text, IntWritable>.Context context)
					throws IOException, InterruptedException {
	
				//line value
				String lineValue = value.toString();
				//split 
//				String lineValue = value.toString(); 
				StringTokenizer stringTokenizer = new StringTokenizer(lineValue);
				
				//iterator
				while(stringTokenizer.hasMoreTokens()) {
					//get word value
					String wordValue = stringTokenizer.nextToken();
					// set key
					mapOutputKey.set(wordValue);
					//out put
					context.write(mapOutputKey, mapOutputValue);
				}
			}
			
			
			
		}
	
	
	
		//step2 : reduce class
		/***
		 * 
		 */
		public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
			
			private  IntWritable reduceOutputValue = new IntWritable();
			
			@Override
			public void reduce(Text key, Iterable<IntWritable> values,Reducer<Text, IntWritable, Text, IntWritable>.Context context)
					throws IOException, InterruptedException {
				//sum tmp
				int sum=0;
				//iterator
				for(IntWritable value : values) {
					// total
					sum += value.get();
				}
				//set value
				reduceOutputValue.set(sum);
				//output
				context.write(key, reduceOutputValue);
			}
					
		}
	
		
		
		//step3 : driver,component job
		public  int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
				//1 get configuration
			Configuration configuration = new Configuration();
			
				//2 create job
			Job job =  Job.getInstance(configuration, this.getClass().getSimpleName());
			
				//run jar 
			job.setJarByClass(this.getClass());
			
				//3 set job
			//input -> map ->reduce -> output
			
				//3.1 input
			Path inPath = new Path(args[0]);
			FileInputFormat.addInputPath(job, inPath);
			
				//3.2 map
			job.setMapperClass(WordCountMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
				//3.3 reduce
			job.setReducerClass(WordCountReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
				//3.4 output
			Path outPath = new Path(args[1]);
			FileOutputFormat.setOutputPath(job, outPath);
			
			//4 submit job
			boolean isSuccess = job.waitForCompletion(true);
			
			return isSuccess? 0 : 1;
			
			
		}
		
		
		//step 5 run program
	 public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		
		 int status = new WordCount().run(args);
		 System.exit(status);
		 
		 
	}
}
