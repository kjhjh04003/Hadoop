package com.hadoopEx.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.hadoopEx.mapper.WordCountMapper;
import com.hadoopEx.reducer.WordCountReducer;

public class WordCount {
	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.out.println("Usege: WordCount <input> <output>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "WordCount"); // mapreduce에 job 명칭 부여

		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true); // 드라이버 끝

	}
}
