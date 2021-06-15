package com.hadoopEx.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoopEx.common.AirlinePerfomanceParser;

public class DepartureDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable outputValue = new IntWritable(1);
	private Text outputKey = new Text();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		AirlinePerfomanceParser parser = new AirlinePerfomanceParser(value); // value는 라인넘버

		outputKey.set(parser.getYear() + "," + parser.getMonth()); // "2012,01", "2012,02"
		if(parser.getDepartureDelayTime() > 0) { // 양수인 이유는 지연된 시간을 사용하기 위해, 음수의 의미는 출발을 빨리했다는 의미
			context.write(outputKey, outputValue);
			
		}
		
	}

}
