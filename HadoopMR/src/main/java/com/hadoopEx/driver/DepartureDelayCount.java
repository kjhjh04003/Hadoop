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

import com.hadoopEx.mapper.DepartureDelayCountMapper;
import com.hadoopEx.reducer.DelayCountReducer;

public class DepartureDelayCount {
	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.out.println("Usege: DepartureDelayCount <input> <output>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "DepartureDelayCount"); // mapreduce에 job 명칭 부여

		job.setJarByClass(DepartureDelayCount.class);
		job.setMapperClass(DepartureDelayCountMapper.class); // 입력되는 레코드가 그냥 레코드가 됨
		job.setReducerClass(DelayCountReducer.class); // 맵에서 출력되는 것이 그대로 리듀스의 출력이 됨, 리듀스 내부적으로 sort가 이루어짐

		// 입력파일 형식과 출력 파일 형식
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// 맵 출력과 리듀스 출력의 key, value 타입 설정
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 입력 파일
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// 출력 파일
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true); // 드라이버 끝
	}
}
