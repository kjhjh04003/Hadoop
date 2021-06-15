package com.hadoopEx.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoopEx.common.AirlinePerfomanceParser;

public class ArrivalDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable outputvalue = new IntWritable(1);
	private Text outputkey = new Text();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		AirlinePerfomanceParser parser = new AirlinePerfomanceParser(value);
		outputkey.set(parser.getYear()+","+parser.getMonth());
		if(parser.getArriveDelayTime()>0) {
			context.write(outputkey, outputvalue);
		}
	}

}
