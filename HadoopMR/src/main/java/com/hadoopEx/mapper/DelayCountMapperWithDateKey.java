package com.hadoopEx.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoopEx.common.AirlinePerfomanceParser;
import com.hadoopEx.common.DateKey;
import com.hadoopEx.counter.DelayCounters;

// 라인번호, 라인 내용, 키 번호, 키 내용

public class DelayCountMapperWithDateKey extends Mapper<LongWritable, Text, DateKey, IntWritable> {
	private final static IntWritable outputvalue = new IntWritable(1); // map 출력값
	private DateKey outputkey = new DateKey(); // map 출력키

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		AirlinePerfomanceParser parser = new AirlinePerfomanceParser(value);

		// 출발과 도착 데이터를 구분할 수 있게 year 앞에 D와 A를 붙여준다.
		if (parser.isDepartureDelayAvailable()) {
			if (parser.getDepartureDelayTime() > 0) {
				outputkey.setYear("D," + parser.getYear());
				outputkey.setMonth(parser.getMonth()); // 출력키(운항연도, 운항월) 설정 형태 = D, 1987, 3
				context.write(outputkey, outputvalue); // 출력 데이터 생성
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
				outputkey.setYear("A," + parser.getYear());
				outputkey.setMonth(parser.getMonth()); // 출력키(운항연도, 운항월) 설정 형태 = A, 1987, 3
				context.write(outputkey, outputvalue); // 출력 데이터 생성
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
