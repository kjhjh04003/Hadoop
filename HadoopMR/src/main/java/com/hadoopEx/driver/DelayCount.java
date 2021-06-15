package com.hadoopEx.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoopEx.mapper.DelayCountMapper;
import com.hadoopEx.reducer.DelayCountReducer;

public class DelayCount extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DelayCount(), args); // 결과가 정상인지 아닌지 츌력 용도
		System.out.println("MR-Job Result : " + res);
	}

	@Override
	public int run(String[] args) throws Exception {
		// GenericOptioinsParser에서 제공하는 파라미터를 제외한 나머지 파라미터 가져오기
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.out.println("Usege: DelayCount <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(getConf(), "DelayCount");

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setJarByClass(DelayCount.class);
		job.setMapperClass(DelayCountMapper.class);
		job.setReducerClass(DelayCountReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
		return 0;
	}

}
