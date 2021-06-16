package com.hadoopEx.common;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

// 맵의 결과가 어떤 리듀스의 입력 데이터로 보내질지를 결정
// 년도의 해시값을 사용해 파티셔너를 결정
public class GroupKeyPartitioner extends Partitioner<DateKey, IntWritable> { // Mapper의 출력 데이터의 키와 값에 해당하는 파라미터  

	@Override
	// 파티셔닝 번호를 조회하는 메서드
	public int getPartition(DateKey key, IntWritable value, int numPartitions) {
		int hash = key.getYear().hashCode();
		int partition = hash % numPartitions;
		return partition;
	}

}
