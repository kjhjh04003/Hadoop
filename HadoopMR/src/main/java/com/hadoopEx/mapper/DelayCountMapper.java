package com.hadoopEx.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoopEx.common.AirlinePerfomanceParser;

public class DelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private String workType; // 출발 지연인지, 도착 지연인지를 구별하는 변수
	private final static IntWritable outputvalue = new IntWritable(1);
	private Text outputkey = new Text();

	// 출발지연인지, 도착지연인지 구별하는 것
	// Mapper 객체가 실행될 때 최초 한번만 실행되는 메서드
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		workType = context.getConfiguration().get("workType"); // Configuration()를 이용하여 workType를 가져와 구별한다.
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		AirlinePerfomanceParser parser = new AirlinePerfomanceParser(value);
		outputkey.set(parser.getYear() + "," + parser.getMonth());

		if (workType.equals("departure")) {
			if (parser.getDepartureDelayTime() > 0) {
				context.write(outputkey, outputvalue);
			}
		} else if (workType.equals("arrival")) {
			if (parser.getArriveDelayTime() > 0) {
				context.write(outputkey, outputvalue);
			}

		}
	}
}
