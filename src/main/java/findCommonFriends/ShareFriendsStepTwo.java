package findCommonFriends;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ShareFriendsStepTwo {
	
	
	/*	文本内容:
	 	C  A,B     C是A,B的粉丝
		E  A,B,C   E是A,B,C的粉丝
		......
		*/

	public static class StepTwoMapper extends Mapper<LongWritable, Text, Text, Text>{

		Text keyText = new Text();
		Text valueText = new Text();
		/***
		 *输入 value的格式    C  A,B  即C是A,B的粉丝，然后将后面的两两配对
		 *
		 */ 
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			
			String fan = line.split("\t")[0];  // C
			String content = line.split("\t")[1]; // A,B
			
			String[] persons = content.split(",");
			
			//将persons进行排序
			Arrays.sort(persons);
			
			valueText.set(fan);  
			
			//C  A,B  C是A,B的粉丝，然后将后面的两两配对
			for(int i =0;i <persons.length;i++) {
				for(int j =i+1;j<persons.length;j++) {
					keyText.set(persons[i] + "," +persons[j]);
					context.write(keyText, valueText);
				}
			}
			/*** 输出内容: 	<key,value> 
			 * 				A,B   	C   A与B的共同粉丝C
					  		A,B		E
					  		A,C		E
					  		B,C		E
					  		......
			*/	
		}
		
	}
	
	
	public static class StepTwoReducer extends Reducer<Text, Text, Text, Text>{

		/***
		 * maper 输出内容:
		 * 	key  	value  
		 * 	A,B   	C 
		 * 	A,B		E
		 * 	A,C		E
		 * ......
		 * reducer分组:
		 * 	key		value
		 * 	A,B		list(C,E,...)    AB 的共同粉丝
		 * 	A,C		list(E,...)
		 * ....
		 * 作为reducer的输入
		 */
		Text valueText = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			StringBuffer sb = new StringBuffer();
			sb.append("[");
			for(Text fan : values) {
				sb.append(fan).append(",");
			}
			 //去掉多余的“,”
            sb.deleteCharAt(sb.length()-1);
            
            sb.append("]");
            
            valueText.set(sb.toString());
            
            context.write(key, valueText);
			
		}
		
	}
	
	
	
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(ShareFriendsStepTwo.class);
		
		job.setMapperClass(StepTwoMapper.class);
		job.setReducerClass(StepTwoReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//4 submit job
		boolean isSuccess = job.waitForCompletion(true);
				
		return isSuccess? 0 : 1;
		
	}
	
	
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		 int status = new ShareFriendsStepTwo().run(args);
		 System.exit(status);
	}
	
}
