/**
 * 
 */
package com.nus.cool.core.cohort.query;

import java.io.Closeable;

import com.nus.cool.core.cohort.schema.CubeSchema;
import com.nus.cool.core.cohort.schema.TableSchema;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;

/**
 * The base interface for all cohort query operators. For the
 * current implementation, we only have two operators:
 * TableFilter and CohortBy.
 * 
 * @author david
 *
 */
public interface Operator extends Closeable {

	void init(CubeSchema cubeSchema, TableSchema tableSchema, CohortQuery query);
	
	void process(MetaChunkRS metaChunk);
	
	boolean isCohortsInCublet();
	
	void process(ChunkRS chunk);
	
}
