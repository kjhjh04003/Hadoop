package com.hadoopEx.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoopEx.common.AirlinePerfomanceParser;
import com.hadoopEx.counter.DelayCounters;

public class DelayCountMapperWithCounter extends Mapper<LongWritable, Text, Text, IntWritable> {
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

		if (workType.equals("departure")) {
			if (parser.isDepartureDelayAvailable()) {
				if (parser.getDepartureDelayTime() > 0) {
					outputkey.set(parser.getYear() + "," + parser.getMonth());
					context.write(outputkey, outputvalue);
				} else if (parser.getDepartureDelayTime() == 0) {
					context.getCounter(DelayCounters.schedualed_departure).increment(1);
				} else if (parser.getDepartureDelayTime() < 0) {
					context.getCounter(DelayCounters.ealy_departure).increment(1);
				}
			} else {
				context.getCounter(DelayCounters.not_available_departure).increment(1);
			}
		} else if (workType.equals("arrival")) {
			if (parser.isArriveDelayAvailable()) {
				if (parser.getArriveDelayTime() > 0) {
					outputkey.set(parser.getYear() + "," + parser.getMonth());
					context.write(outputkey, outputvalue);
				} else if (parser.getArriveDelayTime() == 0) {
					context.getCounter(DelayCounters.scheduled_arrival).increment(1);
				} else if (parser.getArriveDelayTime() < 0) {
					context.getCounter(DelayCounters.ealy_arrival).increment(1);
				}
			} else {
				context.getCounter(DelayCounters.not_available_arrival).increment(1);
			}

		}
	}
}
