/**
 * 
 */
package com.nus.cool.core.cohort.converter;

import org.joda.time.DateTime;
import org.joda.time.Days;

import com.nus.cool.core.cohort.query.DateBase;

/**
 * Convert the input day represented in yyyy-MM-dd to integer
 * which is the number of days past the reference day.
 * 
 * @author david
 *
 */
public class DayIntConverter implements NumericConverter{
	
	private static DayIntConverter converter;
	
	public static DayIntConverter intance() {
		if (converter == null)
			converter = new DayIntConverter();
		return converter;
	}
	
	@Override
	public int toInt(String v) {
		DateTime end = DateBase.FORMATTER.parseDateTime(v);
		return Days.daysBetween(DateBase.BASE, end).getDays();
	}

	@Override
	public String getString(int days) {
		DateTime dt = DateBase.BASE.plusDays(days);
        return DateBase.FORMATTER.print(dt);
		//return fmt.print(dt);
	}

	@Override
	public double toDouble(String v) {
		return toInt(v);
	}

}
