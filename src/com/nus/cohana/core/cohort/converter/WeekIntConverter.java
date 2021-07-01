/**
 * 
 */
package com.nus.cohana.core.cohort.converter;


import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.Weeks;

import com.nus.cohana.core.cohort.query.DateBase;


/**
 * @author david
 *
 */
public class WeekIntConverter implements NumericConverter {

	@Override
	public int toInt(String v) {
		DateTime now = DateBase.FORMATTER.parseDateTime(v);
		return Weeks.weeksBetween(DateBase.BASE, now).getWeeks();
	}

	@Override
	public String getString(int i) {
		DateTime now = DateBase.BASE.plusWeeks(i);
		return now.toLocalDate().withDayOfWeek(DateTimeConstants.MONDAY).toString();
	}

	@Override
	public double toDouble(String v) {
		throw new UnsupportedOperationException();
	}
}
