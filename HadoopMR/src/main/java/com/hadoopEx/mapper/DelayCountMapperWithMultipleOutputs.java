package com.hadoopEx.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoopEx.common.AirlinePerfomanceParser;
import com.hadoopEx.counter.DelayCounters;

public class DelayCountMapperWithMultipleOutputs extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable outputvalue = new IntWritable(1);
	private Text outputkey = new Text();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		AirlinePerfomanceParser parser = new AirlinePerfomanceParser(value);

		if (parser.isDepartureDelayAvailable()) {
			if (parser.getDepartureDelayTime() > 0) {
				outputkey.set("D," + parser.getYear() + "," + parser.getMonth());
				context.write(outputkey, outputvalue);
			} else if (parser.getDepartureDelayTime() == 0) {
				context.getCounter(DelayCounters.schedualed_departure).increment(1);
			} else if (parser.getDepartureDelayTime() < 0) {
				context.getCounter(DelayCounters.ealy_departure).increment(1);
			}
		} else {
			context.getCounter(DelayCounters.not_available_departure).increment(1);

		}
		if (parser.isArriveDelayAvailable()) {
			if (parser.getArriveDelayTime() > 0) {
				outputkey.set("A," + parser.getYear() + "," + parser.getMonth());
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
