package com.hadoopEx.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.hadoopEx.common.DateKey;

public class DelayCountReducerWithDateKey extends Reducer<DateKey, IntWritable, DateKey, IntWritable> {

	private MultipleOutputs<DateKey, IntWritable> mos;
	private DateKey outputkey = new DateKey(); // reduce 출력키
	private IntWritable result = new IntWritable(); // reduce 출력 값

	@Override
	protected void setup(Reducer<DateKey, IntWritable, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		mos = new MultipleOutputs<DateKey, IntWritable>(context);

	}

	@Override
	// 복합키를 입력데이터의 키와 출력 데이터의 키로 사용해야 하므로 파라미터 설정
	protected void reduce(DateKey key, Iterable<IntWritable> values,
			Reducer<DateKey, IntWritable, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// 들어오는 연도가 앞에 D나 A가 잇기 때문에 잘라내는 작업
		String[] columns = key.getYear().split(",");

		int sum = 0;
		// 월값을 백업해두기 위한 변수, 해당 변수에 월값을 백업하지 않으면 12월만 출력되고, 해당 연도의 지연 횟수가 모두 합산되어 출력된다.
		// 월값을 bMonth 변수에 백업해 두어 월별 지연 횟수가 출력될 수 있도록 한다.
		Integer bMonth = key.getMonth();

		if (columns[0].equals("D")) {
			// 월별로 우선 정렬이 되서 들어오면 지연값을 if아래에 있는 sum+=value.get()을 하고 해당 월을 bMonth에 저장한다. 그 후
			// 월이 바뀌면 if문으로 들어와 이전에 작성한 것을 쓰고 다시 sum을 0으로 초기화
			// 다음 월도 동일하게 수행, 마지막 월은 for문 안에있는 sum+=value.get()을 계속 할 것이다. 그러므로 for문을 빠져나와
			// if문을 이용하여 write하는 코드를 작성
			for (IntWritable value : values) {
				if (bMonth != key.getMonth()) { // 백업된 월과 현재 데이터의 월이 일치하지 않을 때
					result.set(sum);
					outputkey.setYear(key.getYear().substring(2));
					outputkey.setMonth(bMonth); // 리듀서의 출력 데이터에 백업된 월의 지연 횟수를 출력
					mos.write("departure", outputkey, result);
					sum = 0; // 다음 순서에 있는 월의 지연 횟수를 합산할 수 있게 지연 횟수 합계를 0으로 초기화
				}
				sum += value.get();
				bMonth = key.getMonth();
			}
			if (key.getMonth() == bMonth) {
				outputkey.setYear(key.getYear().substring(2));
				outputkey.setMonth(bMonth);
				result.set(sum);
				mos.write("departure", outputkey, result);
			}
		} else {
			for (IntWritable value : values) {
				if (bMonth != key.getMonth()) {
					result.set(sum);
					outputkey.setYear(key.getYear().substring(2));
					outputkey.setMonth(bMonth);
					mos.write("arrival", outputkey, result);
					sum = 0;
				}
				sum += value.get();
				bMonth = key.getMonth();
			}
			if (key.getMonth() == bMonth) {
				outputkey.setYear(key.getYear().substring(2));
				outputkey.setMonth(bMonth);
				result.set(sum);
				mos.write("arrival", outputkey, result);
			}
		}

	}

	@Override
	// 맨 마지막에 메모리 정리
	protected void cleanup(Reducer<DateKey, IntWritable, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		mos.close();
	}

}
