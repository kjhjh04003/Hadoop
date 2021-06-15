package com.hadoopEx.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	// 맵에서 나온 결과를 셔플링 해서 키에 대한 갯수의 합을 계산하는 작업
	private IntWritable result = new IntWritable(); // 값을 1로 설정할 수 없기 때문에 우선 빈값으로 설정

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable value : values) { // IntWritable 타입으로 value를 values까지 반복
			sum += value.get(); // value의 값을 얻어와 sum 변수에 더한다.
		}
		result.set(sum);
		context.write(key, result);

	}

}
