package com.hadoopEx.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//파티션 클래스에서 년도의 해시값을 사용했기 때문에 그룹키의 비교는 년도값으로 이루어진다.
//그룹키의 Comparator에서는 각 키의 년도를 비교하며 같은 년도의 데이터를 하나의 Reducer에서 처리하기 위해서다 .
public class GroupKeyComparator extends WritableComparator {
	protected GroupKeyComparator() {
		super(DateKey.class, true);
	}

	@Override
	// 그룹핑된 연도를 비교하는 것
	public int compare(WritableComparable a, WritableComparable b) {
		DateKey k1 = (DateKey) a;
		DateKey k2 = (DateKey) b;

		return k1.getYear().compareTo(k2.getYear());
	}

}
