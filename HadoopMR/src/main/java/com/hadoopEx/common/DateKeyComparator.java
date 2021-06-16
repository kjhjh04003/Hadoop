package com.hadoopEx.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// DateKey들을 비교하여 순서를 정한다.
public class DateKeyComparator extends WritableComparator {
	protected DateKeyComparator() {
		super(DateKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// 두 개의 WritableComparable 객체를 파라미터로 전달받아 DateKey 타입으로 형변환한다.
		// 그래야만 DateKey에 선언한 멤버 변수를 조회할 수 있기 때문
		DateKey k1 = (DateKey) a;
		DateKey k2 = (DateKey) b;

		// 연도를 면저 비교
		int cmp = k1.getYear().compareTo(k2.getYear());
		if (cmp != 0) {
			return cmp; // 연도가 같으면 0, k1이 클 경우 1, k1이 작을경우 -1을 반환
		}
		
		// 연도 비교가 끝나면 월비교(기본 오름차순정렬)
		return k1.getMonth() == k2.getMonth() ? 0 : (k1.getMonth() < k2.getMonth() ? -1 : 1);
	}

}
