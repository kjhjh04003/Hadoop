package com.hadoopEx.common;

import org.apache.hadoop.io.Text;

public class AirlinePerfomanceParser {
	// 특정 컬럼
	private int year;
	private int month;
	private int day;

	private int arriveDelayTime = 0;
	private int departureDelayTime = 0;
	private int distance = 0;

	private boolean arriveDelayAvailable = true;
	private boolean departureDelayAvailable = true;
	private boolean distanceAvailable = true;

	private String uniqueCarrier;

	// 생성자를 이용해 데이터를 하나씩 뽑아낸다.
	public AirlinePerfomanceParser(Text text) {
		try {
			String[] columns = text.toString().split(","); // 콤마로 구분

			year = Integer.parseInt(columns[0]);
			month = Integer.parseInt(columns[1]);
			day = Integer.parseInt(columns[2]);
			uniqueCarrier = columns[5];

			// 데이터가 null인 값을 확인해주어야한다.
			if (!columns[16].equals("")) { // 데이터가 비어있지 않으면
				departureDelayTime = (int) Float.parseFloat(columns[16]);
			} else {
				departureDelayAvailable = false;
			}
			if (!columns[26].equals("")) { // 데이터가 비어있지 않으면
				arriveDelayTime = (int) Float.parseFloat(columns[26]);
			} else {
				arriveDelayAvailable = false;
			}
			if (!columns[37].equals("")) { // 데이터가 비어있지 않으면
				distance = (int) Float.parseFloat(columns[37]);
			} else {
				distanceAvailable = false;
			}
		} catch (Exception e) {

		}
	}

	public int getYear() {
		return year;
	}

	public int getMonth() {
		return month;
	}

	public int getDay() {
		return day;
	}

	public int getArriveDelayTime() {
		return arriveDelayTime;
	}

	public int getDepartureDelayTime() {
		return departureDelayTime;
	}

	public int getDistance() {
		return distance;
	}

	public boolean isArriveDelayAvailable() {
		return arriveDelayAvailable;
	}

	public boolean isDepartureDelayAvailable() {
		return departureDelayAvailable;
	}

	public boolean isDistanceAvailable() {
		return distanceAvailable;
	}

	public String getUniqueCarrier() {
		return uniqueCarrier;
	}

}
