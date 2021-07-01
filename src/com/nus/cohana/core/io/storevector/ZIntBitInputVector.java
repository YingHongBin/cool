package com.nus.cohana.core.io.storevector;

import com.nus.cohana.core.io.InputVector;
import com.nus.cohana.core.lang.Integers;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.LongBuffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import com.google.common.base.MoreObjects;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * pack data into a long buffer in a compact manner
 *
 * @author qingchao
 * 
 */
public class ZIntBitInputVector implements ZIntStore, InputVector {
	private final LongBuffer bitPack;

	private final int capacity;
	private final int bitWidth;
	private final int noValPerPack;
	private final long mask;

	private int pos;
	private long curPack;
	private int packOffset;

	/**
	 *
	 * @param buf
	 *          a byte array containing the compressed data
	 * @param offset
	 *          offset within the buffer
	 * @param len
	 *          length of buffer for storage
	 */
	private ZIntBitInputVector(byte[] buf, int offset, int len) {

		ByteBuffer buffer = ByteBuffer.wrap(buf, offset, len);
		buffer.order(ByteOrder.nativeOrder());
		this.capacity = buffer.getInt();
		this.bitWidth = buffer.getInt();
		this.noValPerPack = 64 / bitWidth;
		this.mask = (bitWidth == 64) ? -1 : (1L << bitWidth) - 1;

		int numOfBytes = getNumOfBytes(capacity, bitWidth);

		if (len < numOfBytes) {
			System.out.printf("len = %d, numOfBytes = %d\n", len, numOfBytes);
			throw new java.nio.BufferUnderflowException();
		}

		// the first 8 bytes of the buffer are for metadata
		// the following bytes are for compressed data, organized as a long buffer
		this.bitPack = ByteBuffer.wrap(buf, offset + 8, numOfBytes - 8)
				.order(ByteOrder.nativeOrder()).asLongBuffer();

		rewind();
	}

	// Tick for quick test
	private ZIntBitInputVector(LongBuffer buf, int capacity, int bitWidth) {
		this.capacity = capacity;
		this.bitWidth = bitWidth;
		this.noValPerPack = 64 / bitWidth;
		this.mask = (bitWidth == 64) ? -1 : (1L << bitWidth) - 1;
		this.bitPack = buf;
	}

	private static int getNumOfBytes(int num, int width) {
		int i = 64 / width;
		int size = (num - 1) / i + 2;
		return size << 3;
	}

	public static ZIntBitInputVector load(ByteBuffer buf) {
		int capacity = buf.getInt();
		int width = buf.getInt();
		int size = getNumOfBytes(capacity, width);

		// David begins here
		int oldLimit = buf.limit();
		buf.limit(buf.position() + size - 8);
		LongBuffer tmpBuffer = buf.asLongBuffer();

		buf.position(buf.position() + size - 8);
		buf.limit(oldLimit);
		return new ZIntBitInputVector(tmpBuffer, capacity, width);
		// end David
	}

	public static ZIntBitInputVector load(byte[] compress) {
		return load(compress, 0, compress.length);
	}

	public static ZIntBitInputVector load(byte[] compress, int offset, int len) {
		return new ZIntBitInputVector(compress, offset, len);
	}

	private long getPack(int pos) {
		int idx = pos / noValPerPack;
		return bitPack.get(idx);
	}

	private void putPack(int pos, long val) {
		bitPack.put(pos / noValPerPack, val);
	}

	@Override
	public int binarySearch(int key) {
		int from = 0;
		int to = capacity - 1;
		int mid;
		int midVal;

		while (from <= to) {
			mid = (from + to) >> 1;
			midVal = get(mid);

			if (key > midVal)
				from = mid + 1;
			else {
				if (key < midVal)
					to = mid - 1;
				else
					return mid;
			}
		}

		return -1;
	}

	public void groupBy(int[] vals, int[] cnt) {
		checkArgument(vals.length == cnt.length);
		long[] extVals = new long[vals.length];
		long mask1 = 0, mask2 = 0;
		int diff = 64 - noValPerPack * bitWidth;
		int width = bitWidth - 1;
		int i, j;

		for (j = 0; j < this.noValPerPack; j++) {
			mask1 = (mask1 << this.bitWidth) + (1 << width) - 1;
			mask2 = (mask2 << this.bitWidth) + (1 << width);
		}

		mask1 <<= diff;
		mask2 <<= diff;

		for (i = 0; i < vals.length; i++) {
			extVals[i] = 0;
			for (j = 0; j < this.noValPerPack; j++) {
				extVals[i] = (extVals[i] << this.bitWidth) + vals[i];
			}
			extVals[i] <<= diff;
			cnt[i] = 0;
		}

		int pos = this.bitPack.position();
		bitPack.rewind();
		long cur;
		long tmp;
		while (bitPack.hasRemaining()) {
			cur = bitPack.get();
			for (i = 0; i < extVals.length; i++) {
				tmp = cur ^ extVals[i];
				tmp += mask1;
				tmp = (~tmp);
				tmp &= mask2;
				cnt[i] += Long.bitCount(tmp);
			}
		}
		this.bitPack.position(pos);
	}

	@Override
	public int size() {
		return this.capacity;
	}

	@Override
	public int sizeInByte() {
		return getNumOfBytes(this.capacity, this.bitWidth);
	}

	@Override
	public boolean hasNext() {
		return pos < capacity;
	}

	public long nextLong() {
		if (packOffset < bitWidth) {
			curPack = bitPack.get();
			packOffset = 64;
		}

		pos++;
		packOffset -= bitWidth;
		long val = ((curPack >>> packOffset) & mask);
		return val;
	}

	@Override
	public int next() {
		return (int) nextLong();
	}

	@Override
	public void rewind() {
		pos = 0;
		bitPack.rewind();
		packOffset = 0;
	}

	@Override
	public int get(int pos) {
		if (pos >= capacity)
			throw new java.lang.IndexOutOfBoundsException();

		int offset = 64 - (1 + pos % noValPerPack) * bitWidth;
		long pack = getPack(pos);
		long val = ((pack >>> offset) & mask);
		return (int) val;
	}

	/**
	 * @brief Suppose noValPerPack is 5. So each pack has 12 values. suppose now
	 *        we are trying to put a value of 10 (01010) to pos 100. We first need
	 *        to get the right pack, which is the 100/12 = 8 pack, and set the
	 *        100%12 = 4th value to 10. In order to do so, we first clear the
	 *        corresponding 5 bits (i.e., 20-24), which can be done by masking the
	 *        pack with ((1<<4*5)-1)<<(64-4*5) | ((1<<(64 - 5*5)) - 1) and then OR
	 *        it with (10<<(64-5*5))
	 *
	 */
	public void put(int pos, long val) {
		if (pos >= capacity)
			throw new java.lang.IndexOutOfBoundsException();

		long one = 1;
		long pack = getPack(pos);
		int i = pos % noValPerPack;
		int offset = i * bitWidth;
		int shift = 64 - offset;
		long headMask = (((one << offset) - 1) << shift);
		long tailMask = (one << (shift - bitWidth)) - 1;
		long mask = headMask | tailMask;
		pack &= mask;
		pack |= (val << (shift - bitWidth));
		putPack(pos, pack);
	}

	@Override
	public void put(int pos, int val) {
		put(pos, (long) val);
	}

	@Override
	public void put(int[] valueToCompress, int offset, int numOfVal) {
		for (int i = 0; i < numOfVal; i++) {
			put(i, valueToCompress[i + offset]);
		}
	}

	@Override
	public void readFrom(ByteBuffer buf) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeTo(DataOutput out) throws IOException {
		// output the data according to the native byte order of platform
		boolean bLittle = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
		out.writeInt(Integers.toNativeByteOrder(capacity));
		out.writeInt(Integers.toNativeByteOrder(bitWidth));

		for (int i = 0; i < bitPack.limit(); i++) {
			long v = bitPack.get(i);
			out.writeLong(bLittle ? Long.reverseBytes(v) : v);
		}
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add("numOfVal", this.capacity)
				.add("bitWidth", this.bitWidth)
				.add("buffer length", this.bitPack.limit())
				.add("cur pos", this.pos)
				.add("curPack", curPack)
				.add("offset" , this.packOffset).toString();

	}

	@Override
	public int find(int key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int sizeInBytes() {
		return getNumOfBytes(this.capacity, this.bitWidth);
	}

	@Override
	public void skipTo(int pos) {
		if (pos >= capacity)
			throw new java.lang.IndexOutOfBoundsException();
				
		this.pos = pos;
		this.packOffset = 64 - (pos % noValPerPack) * bitWidth;
		//this.curPack = getPack(pos);	
		this.bitPack.position(pos/noValPerPack);
		this.curPack = bitPack.get();
	}
}
