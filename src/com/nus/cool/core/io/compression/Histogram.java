/**
 * 
 */
package com.nus.cool.core.io.compression;

/**
 * @author David
 *
 */
public class Histogram {

	private int rawSize;

	private boolean sorted;

	private int uniqueValues;

	private int numOfValues;
	
	private CompressType type;
	
	private long max;
	
	private long min;
	
	public static class HistogramBuilder {

		private int origLengthInBytes;

		private boolean sorted = false;

		private int uniqueValues;

		private int numOfValues;

		private CompressType type;
		
		private long max;
		
		private long min;
		
		public HistogramBuilder sorted(boolean sorted) {
			this.sorted = sorted;
			return this;
		}
		
		public HistogramBuilder rawSize(int size) {
			this.origLengthInBytes = size;
			return this;
		}
		
		public HistogramBuilder uniqueValues(int uniqueValues) {
			this.uniqueValues = uniqueValues;
			return this;
		}
		
		public HistogramBuilder count(int numOfValues) {
			this.numOfValues = numOfValues;
			return this;
		}
		
		public HistogramBuilder compressType(CompressType type) {
			this.type = type;
			return this;
		}
		
		public HistogramBuilder max(long max) {
			this.max = max;
			return this;
		}
		
		public HistogramBuilder min(long min) {
			this.min = min;
			return this;
		}

		public Histogram build() {
			return new Histogram(sorted, uniqueValues, numOfValues,
					origLengthInBytes, type, min, max);
		}
	}
	
	public static HistogramBuilder builder() {
		return new HistogramBuilder();
	}
	
	public Histogram(boolean sorted, int uniqueValues, int numOfValues,
			int origLengthInBytes, CompressType type, long min, long max) {
		this.sorted = sorted;
		this.uniqueValues = uniqueValues;
		this.numOfValues = numOfValues;
		this.rawSize = origLengthInBytes;
		this.type = type;
		this.min = min;
		this.max = max;
	}
	
	public boolean isSorted() {
		return this.sorted;
	}
	
	public int getNumberOfUniqueValues() {
		return this.uniqueValues;
	}
	
	public int count() {
		return this.numOfValues;
	}
	
	public int rawSize() {
		return this.rawSize;
	}
	
	public CompressType compressType() {
		return this.type;
	}
	
	public long max() {
		return max;
	}
	
	public long min() {
		return min;
	}
	
	
}
