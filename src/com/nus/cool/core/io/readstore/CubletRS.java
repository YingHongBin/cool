/**
 * 
 */
package com.nus.cool.core.io.readstore;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import com.nus.cool.core.io.compression.SimpleBitSetCompressor;
import jersey.repackaged.com.google.common.collect.Lists;

import com.google.common.primitives.Ints;

import com.nus.cool.core.cohort.schema.TableSchema;
import com.nus.cool.core.io.Input;

/**
 * Read Cublet Store
 * RS = Read Store
 * TODO: current implementation is only for cohort cublet.
 * May change into an interface when reading OLAP cublet is needed.
 * 
 * @author david, xiezl
 *
 */
public class CubletRS implements Input {
	
	private TableSchema schema;
	
	private MetaChunkRS metaChunk;
	
	private List<ChunkRS> dataChunks = Lists.newArrayList();

	private List<BitSet> bitSets = new ArrayList<>();

	private int limit;

	private String file;

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
	}

	public int getLimit() {
		return this.limit;
	}

	public List<BitSet> getBitSets() {
		return this.bitSets;
	}

	public CubletRS(TableSchema schema) {
		this.schema = checkNotNull(schema);
	}
	
	public MetaChunkRS getMetaChunk() {
		return this.metaChunk;
	}
	
	public List<ChunkRS> getDataChunks() {
		return this.dataChunks;
	}

	@Override
	public void readFrom(ByteBuffer buf) {
		int end = buf.limit();
		this.limit = end;
		int headOffset;
		buf.position(end - Ints.BYTES);
		int tag = buf.getInt();
		if (tag != 0) {
			headOffset = tag;
		} else {
			buf.position(end - Ints.BYTES - Ints.BYTES);
			int size = buf.getInt();
			buf.position(end - Ints.BYTES - Ints.BYTES - Ints.BYTES);
			end = buf.getInt();
			buf.position(end -Ints.BYTES);
			headOffset = buf.getInt();
			buf.position(end);
			for (;size >0; size--) bitSets.add(SimpleBitSetCompressor.read(buf));
		}

		// Get header offset
		// getInt() would read next four bytes at this buffer's current position and increment the position by four
		// so this function convert four bytes to an int value
		// head offset is the starting position of header, but not length of header
		//int headOffset = buf.getInt();
		// Set buffer position at header
		buf.position(headOffset);

		// Cublet header
		/* -------------------------------------------
		   |#chunks|chunk.1 offset|...|chunk.n offset|
		   -------------------------------------------
		 */
		// Get #chunks from header
		int chunks = buf.getInt();
		int[] chunkOffsets = new int[chunks];
		// Get all chunk header offsets from header
		for(int i = 0; i < chunks; i++) {
			int tmp = buf.getInt();
			chunkOffsets[i] = tmp;
			//System.out.println(tmp);
			//chunkOffsets[i] = buf.getInt();
		}

		this.metaChunk = new MetaChunkRS(schema);
		// Get metaChunk header offset from the first chunk header offset
		// Notes: chunk header offset records chunk's header's position
		// Notes: chunk header offsets include metachunk
		buf.position(chunkOffsets[0]);
		int chunkHeadOffset = buf.getInt();
		// Get metaChunk data from metaChunk header
		buf.position(chunkHeadOffset);
		this.metaChunk.readFrom(buf);

        // Get chunk data
		for(int i = 1; i < chunks; i++) {
			ChunkRS dataChunk = new ChunkRS(schema);
			// Get chunk header offset
			buf.position(chunkOffsets[i]);
			chunkHeadOffset = buf.getInt();
			// Get chunk header 
			buf.position(chunkHeadOffset);
			dataChunk.readFrom(buf);
			dataChunks.add(dataChunk);
		}
	}

}
