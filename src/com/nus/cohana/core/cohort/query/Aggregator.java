/**
 * 
 */
package com.nus.cohana.core.cohort.query;

import java.util.BitSet;

import com.nus.cohana.core.io.InputVector;

/**
 * Calculate the users behavior metric after they are born. The 
 * interface is special designed for cohort query. Each iteration
 * processes events of a single user after he/she is born.
 * 
 * We only consider cohort users by a single field.
 * 
 * @author david
 *
 */
public interface Aggregator {
	
	/**
	 * Initialize the aggregator.
	 *  
	 * @param metricVec
	 * @param eventDayVec
	 * @param maxAges
	 * @param from first age to consider
	 * @param to last age to consider
	 * @param ageDivider
	 */
	void init(InputVector metricVec, InputVector eventDayVec, int maxAges, int from, int to, int ageDivider);
	
	void processUser(BitSet hitBV, int sinceDay, int start, int end, long[] row);

	void complete();
	
}
