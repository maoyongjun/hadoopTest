package org.foxconn.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapperReduceTest extends Configuration implements Tool{
	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new MapperReduceTest(), args);
		
		Job wordCountJob = Job.getInstance(new MapperReduceTest().getConf());
		
		//重要：指定本job所在的jar包
		wordCountJob.setJarByClass(MapperReduceTest.class);
		
		//设置wordCountJob所用的mapper逻辑类为哪个类
		wordCountJob.setMapperClass(WordCountMapper.class);
		//设置wordCountJob所用的reducer逻辑类为哪个类
		wordCountJob.setReducerClass(WordCountReducer.class);
		
		//设置map阶段输出的kv数据类型
		wordCountJob.setMapOutputKeyClass(Text.class);
		wordCountJob.setMapOutputValueClass(IntWritable.class);
		
		//设置最终输出的kv数据类型
		wordCountJob.setOutputKeyClass(Text.class);
		wordCountJob.setOutputValueClass(IntWritable.class);
		
		//设置要处理的文本数据所存放的路径
		FileInputFormat.setInputPaths(wordCountJob, "hdfs://192.168.146.158:9000/wordcount/srcdata/");
		FileOutputFormat.setOutputPath(wordCountJob, new Path("hdfs://192.168.146.158:9000/wordcount/output/"));
		
		//提交job给hadoop集群
		wordCountJob.waitForCompletion(true);

	}
	
	public static  class WordCountMapper extends Mapper<Object, Text, Object, IntWritable>{

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Object, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(String str:value.toString().split(" ")){
				context.write(str, new IntWritable(1));
			}
			
		}
		
		
	}
	public static class WordCountReducer extends Reducer<Object, IntWritable, Object,IntWritable>{
		
		
		@Override
		protected void reduce(Object arg0, Iterable<IntWritable> arg1,
				Reducer<Object, IntWritable, Object, IntWritable>.Context arg2)
				throws IOException, InterruptedException {
			int total=0;
			super.reduce(arg0, arg1, arg2);
			for(IntWritable value:arg2.getValues()){
				total= total+ value.get();
			}
			arg2.write(arg0, new IntWritable(total));
		}
		
	}
	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return  new Configuration();
	}
	@Override
	public int run(String[] args) throws Exception {
		
		return 0;
	}
	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		
	}
	
	
	
}
