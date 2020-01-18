/**
 * 
 */
package com.nus.cool.core.lang;

/**
 * Missing functions in java Arrays
 * 
 * @author david
 *
 */
public class MoreArrays {
	
	public static int max(int[] vec) {
		int max = Integer.MIN_VALUE;
		
		for(int v : vec)
			if(v > max)
				max = v;
		
		return max;
	}
	
	public static int min(int[] vec) {
		int min = Integer.MAX_VALUE;
		
		for(int v : vec)
			if(v < min)
				min = v;
		
		return min;
	}
}
