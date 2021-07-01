/**
 * 
 */
package com.nus.cohana.core.cohort.query;

import java.io.Closeable;

import com.nus.cohana.core.cohort.schema.CubeSchema;
import com.nus.cohana.core.cohort.schema.TableSchema;
import com.nus.cohana.core.io.readstore.ChunkRS;
import com.nus.cohana.core.io.readstore.MetaChunkRS;

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
