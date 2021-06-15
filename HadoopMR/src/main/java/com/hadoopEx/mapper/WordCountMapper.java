package com.hadoopEx.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1); // 단어 갯수 변수
	private Text word = new Text(); // 단어 변수

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		StringTokenizer strToken = new StringTokenizer(value.toString()); // 하둡에서 사용되는 데이터 타입을 자바에서 사용되는 데이터 타입으로 변환하는
																			// 작업
		// map 함수 역할
		while (strToken.hasMoreTokens()) { // 자른 단어가 존재한다면
			word.set(strToken.nextToken());
			context.write(word, one); // map과 하둡의 연결장치???context로 확인
		}
	}

}
