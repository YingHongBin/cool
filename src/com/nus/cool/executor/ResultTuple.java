/**
 * 
 */
package com.nus.cool.executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author david
 *
 */
public class ResultTuple {
	
	private String cohort;
	
	private int age;
	
	private long measure;
	
	public ResultTuple() {}
	
	public ResultTuple(String cohort, int age, long measure) {
		this.cohort = checkNotNull(cohort);
		this.age = age;
		this.measure = measure;
	}

	/**
	 * @return the cohort
	 */
	public String getCohort() {
		return cohort;
	}

	/**
	 * @param cohort the cohort to set
	 */
	public void setCohort(String cohort) {
		this.cohort = cohort;
	}

	/**
	 * @return the age
	 */
	public int getAge() {
		return age;
	}

	/**
	 * @param age the age to set
	 */
	public void setAge(int age) {
		this.age = age;
	}

	/**
	 * @return the measure
	 */
	public long getMeasure() {
		return measure;
	}

	/**
	 * @param measure the measure to set
	 */
	public void setMeasure(long measure) {
		this.measure = measure;
	}

	public static List<ResultTuple> merge(List<ResultTuple> resultTuples) {
		Map<String, Map<Integer, Long>> resMap = new HashMap<>();
		for (ResultTuple res : resultTuples) {
			if (resMap.get(res.cohort) == null) {
				Map<Integer, Long> map = new HashMap<>();
				map.put(res.age, res.measure);
				resMap.put(res.cohort, map);
			} else if (resMap.get(res.cohort).get(res.age) == null) {
				Map<Integer, Long> map = resMap.get(res.cohort);
				map.put(res.age, res.measure);
			} else {
				Map<Integer, Long> map = resMap.get(res.cohort);
				map.put(res.age, res.measure + map.get(res.age));
			}
		}
		List<ResultTuple> results = new ArrayList<>();
		for (Map.Entry<String, Map<Integer, Long>> entry : resMap.entrySet()) {
			String cohort = entry.getKey();
			Map<Integer, Long> map = entry.getValue();
			for (Map.Entry<Integer, Long> entry1 : map.entrySet()) {
				int age = entry1.getKey();
				long measure = entry1.getValue();
				ResultTuple res = new ResultTuple(cohort, age, measure);
				results.add(res);
			}
		}
		return  results;
	}
}
