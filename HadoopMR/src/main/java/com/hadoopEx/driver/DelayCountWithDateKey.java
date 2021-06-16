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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoopEx.common.DateKey;
import com.hadoopEx.common.DateKeyComparator;
import com.hadoopEx.common.GroupKeyComparator;
import com.hadoopEx.common.GroupKeyPartitioner;
import com.hadoopEx.mapper.DelayCountMapperWithDateKey;
import com.hadoopEx.reducer.DelayCountReducerWithDateKey;

public class DelayCountWithDateKey extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DelayCountWithDateKey(), args);
		System.out.println("MR-Job Result:" + res);
	}

	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.out.println("Usege: DelayCountWithDateKey <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(getConf(), "DelayCountWithDateKey");

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setJarByClass(DelayCountWithDateKey.class);
		job.setMapperClass(DelayCountMapperWithDateKey.class);
		job.setReducerClass(DelayCountReducerWithDateKey.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(DateKey.class);
		job.setOutputValueClass(IntWritable.class);

		MultipleOutputs.addNamedOutput(job, "departure", TextOutputFormat.class, DateKey.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "arrival", TextOutputFormat.class, DateKey.class, IntWritable.class);

		// 추가
		job.setPartitionerClass(GroupKeyPartitioner.class);
		job.setGroupingComparatorClass(GroupKeyComparator.class);
		job.setSortComparatorClass(DateKeyComparator.class);
		job.setMapOutputKeyClass(DateKey.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.waitForCompletion(true);
		return 0;
	}

}
