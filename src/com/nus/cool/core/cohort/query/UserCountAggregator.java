/**
 * 
 */
package com.nus.cool.core.cohort.query;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.BitSet;

import com.nus.cool.core.io.InputVector;

/**
 * For the moment, we only consider cohort user by a single field.
 * 
 * @author david
 *
 */
public class UserCountAggregator implements Aggregator {
	
	private InputVector eventDayVec;
	
	private BitSet mask;
	
	private int ageDivider;
	
	private int from;
	
	private int to;
	
	@Override
	public void init(InputVector metricVec, InputVector eventDayVec, int maxAges,
			int from, int to, int ageDivider) {
		checkArgument(from >= 0 && from <= to);
		this.eventDayVec = checkNotNull(eventDayVec);
		this.mask = new BitSet(maxAges);
		this.ageDivider = ageDivider;
		this.from = from;
		this.to = to;
	}
	
	@Override
	public void processUser(BitSet hitBV, int sinceDay, int start, int end, long[] row) {
		// row[0] is the cohort size, just ignore it !!!
		mask.clear();
		eventDayVec.skipTo(start);
		
		for(int i = start; i < end; i++) {
			int eventDay = eventDayVec.next();
			if(hitBV.get(i)) {
				int age = (eventDay - sinceDay) / ageDivider;
				if(age <= 0 || age >= row.length || mask.get(age))
					continue;
				
				if(age < from )
					continue;				
				else if(age > to)
					break;
				
				row[age]++;
				mask.set(age);

				// Just select the first age event
				break;
			}
		}		
	}

	@Override
	public void complete() {
	}

}
