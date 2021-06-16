package com.hadoopEx.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

// 복합키 만들기
public class DateKey implements WritableComparable<DateKey> { // 가본 자바에는 WitableComparable은 없다.
	private String year;
	private Integer month;

	// 기본 생성자(빈 생성자)
	public DateKey() {

	}

	// 생성자를 이용한 초기화 작업
	public DateKey(String year, Integer month) {
		this.year = year;
		this.month = month;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public Integer getMonth() {
		return month;
	}

	public void setMonth(Integer month) {
		this.month = month;
	}

	@Override
	// 키를 output에 출력해준다.
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, year); // year 출력
		out.writeInt(month); // month 출력

	}

	@Override
	// 데이터를 통해 키를 만들어 준다.
	public void readFields(DataInput in) throws IOException {
		year = WritableUtils.readString(in);
		month = in.readInt();
	}

	@Override
	// 키 값들을 비교한다.
	public int compareTo(DateKey key) {
		int result = year.compareTo(key.year); // 현재 year와 새로 받은 year와 비교하는 것
		if (result == 0) { // year가 동일하면 월을 비교
			result = month.compareTo(key.month);
		}

		return result;
	}

	@Override
	// mapper와 reducer에서 해당 키를 toString으로 호출해서 사용하기 때문에 오버라이드 하는 것
	public String toString() {
		// StringBuilder를 만들고 year을 append하고 , 찍고 month를 append하고 그것을 문자열로 변환
		return new StringBuilder().append(year).append(",").append(month).toString();
	}

}
