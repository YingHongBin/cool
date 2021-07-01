/**
 * 
 */
package com.nus.cohana.core.io.storevector;

import java.nio.ByteBuffer;

import com.nus.cohana.core.io.InputVector;

/**
 * @author david
 *
 */
public class RLEInputVector implements InputVector {
	
	public static class Block {

		public int value;	//

		public int off;

		public int len;

		@Override
		public String toString() {
			return String.format("(%d, %d, %d)", value, off, len);
		}

	}

	private ByteBuffer buf;

	private int blks;

	private int curBlk;

	private int boff;

	private int bend;

	private int bval;

	private int read(int width) {
		int res;
		switch (width) {
		case 1:
			res = buf.get() & 0xff;
			break;
		case 2:
			res = buf.getShort() & 0xffff;
			break;
		case 3:
		case 0:
			res = buf.getInt();
			break;
		default:
			throw new java.lang.IllegalArgumentException("incorrect number of bytes");
		}
		return res;

	}

	private final void readNextBlock() {
		int b = buf.get();
		bval = read((b >> 4) & 3);
		boff = read((b >> 2) & 3);
		bend = boff + read((b >> 0) & 3);
		curBlk++;
	}

	@Override
	public void readFrom(ByteBuffer buf) {
		int zLen = buf.getInt();
		this.blks = buf.getInt();

		int oldLimit = buf.limit();
		int newLimit = buf.position() + zLen;
		buf.limit(newLimit);
		this.buf = buf.slice().order(buf.order());
		buf.position(newLimit);
		buf.limit(oldLimit);
	}

	@Override
	public int find(int key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int get(int index) {
		int offset = this.boff;
		this.skipTo(index);
		int v = next();
		boff = offset;
		return v;
	}

	@Override
	public boolean hasNext() {
		return curBlk < blks || boff < bend;
	}

	@Override
	public int next() {
		if(boff < bend) {
			boff++;
			return bval;
		}
		
		// Read next blk
		readNextBlock();
		boff++;
		return bval;
	}

	@Override
	public int size() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int sizeInBytes() {
		return 0;
	}

	@Override
	public void skipTo(int pos) {

        if (pos < this.boff) {
            this.buf.rewind();
            this.curBlk = 0;
            readNextBlock();
        }

        while (pos >= this.bend && this.curBlk < this.blks) {
            readNextBlock();
        }

        if (pos >= this.bend) {
            throw new java.lang.IllegalArgumentException("too large pos param");
        }

        this.boff = pos;
	}
	
	public void nextBlock(Block blk) {
		if(boff < bend) {
			// Return current block
			blk.value = bval;
			blk.off = boff;
			blk.len = bend - boff;
			boff = bend;
			return;
		}
		
		readNextBlock();
		blk.value = bval;
		blk.off = boff;
		blk.len = bend - boff;
		boff = bend;
	}

}
