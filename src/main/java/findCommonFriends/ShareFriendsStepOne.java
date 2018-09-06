package findCommonFriends;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ShareFriendsStepOne {
	
	/***
	 * mapper
	 *输入 key 偏移量(LongWritable),  value 每行的文本数据 (Text)  A:B,C,D,F,E,O
	 *输出<key,vlaue>   <B,A>,<C,A>,<D,A>,<F,A>,<E,A>,<O,A>
	 */
	
	public static class  StepOneMapper extends Mapper<LongWritable,Text,Text,Text>{

		Text keyText = new Text();
		Text valueText = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString(); //A:B,C,D,F,E,O
			//以:进行分割
			String person = line.split(":")[0];  //"A"
			String content = line.split(":")[1]; //"B,C,D,F,E,O"
			
			//该person下的所有fans  
			String [] fans = content.split(","); //[B,C,D,F,E,O]
			//设置输出value
			valueText.set(person); // key() -> value(A)
			//
			for(int i=0;i<fans.length;i++) {
				keyText.set(fans[i]);  
				context.write(keyText, valueText);
				//<B,A>,<C,A>,<D,A>,<F,A>,<E,A>,<O,A>
			}
		}
		
	}
	
	/***
	 * reducer
	 */
	public static class StepOneReducer extends Reducer<Text, Text, Text, Text>{

		/***分组group 
		maper 输出<key,vlaue>  <粉丝,人> 
		reducer 分组过程:
			<B,A>,<C,A>,<D,A>,<F,A>,<E,A>,<O,A> 
			<A,B>,<C,B>,<E,B>,<K,A>,...
			将相同key的value放在一起
			<C,list(A,B)>,<E,list(A,B,C)>,... ===  C是A,B的粉丝，E是A,B,C的粉丝 ...
		reducer 输入  <C,list(A,B)>,<E,list(A,B,C)>,...
		
		***/
		
		Text valueText = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			StringBuffer sb = new StringBuffer();
			for(Text person : values) {
				sb.append(person).append(",");
			}
			//最后多的"，" 消除
			String outPerson =sb.substring(0,sb.length()-1);
			
			valueText.set(outPerson);
			context.write(key, valueText);
			
		/*	C  A,B   
			E  A,B,C
			......
			*/
		}
		
		
		
	}

	
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,this.getClass().getSimpleName());
		
		job.setJarByClass(ShareFriendsStepOne.class);
		
		job.setMapperClass(StepOneMapper.class);
		job.setReducerClass(StepOneReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//4 submit job
		boolean isSuccess = job.waitForCompletion(true);
		
		return isSuccess? 0 : 1;
		
	}
	
	
	//step 5 run program
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
	
	 int status = new ShareFriendsStepOne().run(args);
	 System.exit(status);
	 
	 
}
	
	
}
