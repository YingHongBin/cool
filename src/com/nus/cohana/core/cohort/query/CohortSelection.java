/**
 * 
 */
package com.nus.cohana.core.cohort.query;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.nus.cohana.core.cohort.schema.FieldType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nus.cohana.core.cohort.filter.AgeFieldFilter;
import com.nus.cohana.core.cohort.filter.BirthFilter;
import com.nus.cohana.core.cohort.filter.FieldFilter;
import com.nus.cohana.core.cohort.filter.FieldFilterFactory;
import com.nus.cohana.core.cohort.query.FieldSet.FieldSetType;
import com.nus.cohana.core.cohort.schema.CubeSchema;
import com.nus.cohana.core.cohort.schema.TableSchema;
import com.nus.cohana.core.io.InputVector;
import com.nus.cohana.core.io.readstore.ChunkRS;
import com.nus.cohana.core.io.readstore.FieldRS;
import com.nus.cohana.core.io.readstore.MetaChunkRS;
import com.nus.cohana.core.io.readstore.MetaFieldRS;

/**
 * Birth selection & age selection operator. We only use TableSchema now.
 * 
 * @author david
 *
 */
public class CohortSelection implements Operator {
	
	private static Log LOG = LogFactory.getLog(CohortSelection.class);
	
	private FieldFilterFactory filterFactory = new FieldFilterFactory();
		
	private Map<String, FieldFilter> birthFilters = new HashMap<>();
	
	private Map<String, FieldFilter> ageFilters = new HashMap<>();
	
	private Map<String, FieldRS> birthFilterFields = new HashMap<>();
	
	private Map<String, FieldRS> ageFilterFields = new HashMap<>();
	
	private FieldFilter appFilter;
	
	private TableSchema tableSchema;
	
	private boolean bUserActiveCublet;
	
	private boolean bAgeActiveCublet;
	
	private boolean bUserActiveChunk;
	
	private boolean bAgeActiveChunk;
	
	private FieldFilter ageFilter;

	@Override
	public void close() throws IOException {
	}

	@Override
	public void init(CubeSchema cubeSchema, TableSchema tableSchema,
			CohortQuery query) {
		throw new UnsupportedOperationException();
	}
	
	public void init(TableSchema tableSchema, SigmodCohortQuery q) {
		this.tableSchema = checkNotNull(tableSchema);
		q = checkNotNull(q);
		
		// Check whether appKey is valid
		String app = q.getAppKey();
		appFilter = filterFactory.create(
				tableSchema.getFieldSchema(tableSchema.getAppKeyField()), null, Arrays.asList(app));
		
		// Process birth selector
		List<FieldSet> birthSelectors =  q.getBirthSelection();
		for(FieldSet fs : birthSelectors) {
			// neglect birth selector using Birth function
			if (fs.getFilterType() == FieldSetType.Birth)
				continue;
			
			String fieldName = fs.getCubeField();
			birthFilters.put(fieldName, filterFactory.create(tableSchema.getFieldSchema(fieldName), null, fs.getValues()));
		}
		
		// Process age selectors
		List<FieldSet> ageSelectors = q.getAgeSelection();
		for(FieldSet fs : ageSelectors) {
			String fieldName = fs.getCubeField();
			if(fieldName.equals("Age")) {
				if (fs.getFilterType() == FieldSetType.Birth) 
					ageFilter = new AgeFieldFilter(Arrays.asList("0|0"));
				else
					ageFilter = new AgeFieldFilter(fs.getValues());
				continue;
			}

			if (fs.getFilterType() == FieldSetType.Birth) {
				ageFilters.put(fieldName, new BirthFilter());
				continue;
			}			
	
			// TODO: change filterFactory code for compatibility 
			ageFilters.put(fieldName, filterFactory.create(tableSchema.getFieldSchema(fieldName), null, fs.getValues()));
		}
	}
	
	@JsonIgnore
	public FieldFilter getAppFieldFilter() {
		return this.appFilter;
	}
	
	public FieldFilter getBirthFieldFilter(String fieldName) {
		return birthFilters.get(fieldName);
	}
	
	public FieldFilter getAgeFieldFilter(String fieldName) {
		return ageFilters.get(fieldName);
	}
	
	@JsonIgnore
	public FieldFilter getAgeFieldFilter() {
		return this.ageFilter;
	}

	@Override
	public void process(MetaChunkRS metaChunk) {
		bUserActiveCublet = true;
		bAgeActiveCublet = true;
		
		// Process the app filter
		boolean bAccept = appFilter.accept(metaChunk.getMetaField(tableSchema.getAppKeyField(), FieldType.AppKey));
		if(bAccept == false) {
			LOG.info("No app is found in the cublet");
			bUserActiveCublet = false;
			return;
		}
		
		// Process birth filter
		for(Map.Entry<String, FieldFilter> entry : birthFilters.entrySet()) {
			MetaFieldRS metaField = metaChunk.getMetaField(entry.getKey());
			bUserActiveCublet &= entry.getValue().accept(metaField);
		}
		
		// Process age filter
		for(Map.Entry<String, FieldFilter> entry : ageFilters.entrySet()) {
			MetaFieldRS metaField = metaChunk.getMetaField(entry.getKey());
			bAgeActiveCublet &= entry.getValue().accept(metaField);
		}
	}

	@Override
	public boolean isCohortsInCublet() {
		return false;
	}

	@Override
	public void process(ChunkRS chunk) {
		birthFilterFields.clear();
		ageFilterFields.clear();
		bUserActiveChunk = true;
		bAgeActiveChunk = true;
		
		// Process the app filter
		boolean bAccept = appFilter.accept(chunk.getField(tableSchema.getAppKeyField()));
		if(bAccept == false) {
			LOG.info("No app is found in the data chunk");
			bUserActiveChunk = false;
			return;
		}
		
		// Process birth filter field
		for(Map.Entry<String, FieldFilter> entry : birthFilters.entrySet()) {
			FieldRS field = chunk.getField(entry.getKey());
			bUserActiveChunk &= entry.getValue().accept(field);
			birthFilterFields.put(entry.getKey(), field);
		}
		
		// Process age filter field
		for(Map.Entry<String, FieldFilter> entry : ageFilters.entrySet()) {
			FieldRS field = chunk.getField(entry.getKey());
			bAgeActiveChunk &= entry.getValue().accept(field);
			ageFilterFields.put(entry.getKey(), field);
		}		

	}
	
	/**
	 * Select user based on the birth tuple at birth off
	 * @param birthOff
	 * @return true for qualified, false for filtering
	 */
	public boolean selectUser(int birthOff) {
		boolean bSelected = true;
		for(Map.Entry<String, FieldFilter> entry : birthFilters.entrySet()) {
			FieldRS field = birthFilterFields.get(entry.getKey());
			InputVector fieldIn = field.getValueVector();
			fieldIn.skipTo(birthOff);
			bSelected &= entry.getValue().accept(fieldIn.next());
			if(bSelected == false)
				break;		
		}
		
		// let ageBirthFilterFields fetch the value of the birth tuple
		if (bSelected) {
			for (Map.Entry<String, FieldFilter> entry : ageFilters.entrySet()) {
				if (entry.getValue() instanceof BirthFilter)
					((BirthFilter)entry.getValue()).setCurUserBirthOff(birthOff);
			}
		}
		
		return bSelected;
	}	

	
	/**
	 * Select age activity tuples bounded by [ageOff, ageEnd)
	 * @param ageOff the start position of age tuples
	 * @param ageEnd the end position of age tuples
	 * @param bs the hit position list of all qualified tuples
	 */
	public void selectAgeActivities(int ageOff, int ageEnd, BitSet bs) {
		// Columnar processing strategy ...
		for(Map.Entry<String, FieldFilter> entry : ageFilters.entrySet()) {
			FieldRS field = ageFilterFields.get(entry.getKey());
			InputVector fieldIn = field.getValueVector();
			fieldIn.skipTo(ageOff);
			FieldFilter ageFilter = entry.getValue();
			
//			for (int i = ageOff; i < ageEnd; i++) {
//				if (!ageFilter.accept(fieldIn.next()))
//				  bs.clear(i);
//			}

			if ((bs.cardinality() << 1) >= (ageEnd - ageOff)) {
				for (int i = ageOff; i < ageEnd; i++) {
					if (!ageFilter.accept(fieldIn.next()))
					  bs.clear(i);
				}
			} else {
				//skip unqualified age tuples if they only account for 
			    //less than half of the total population		  
				int off = bs.nextSetBit(ageOff);
				while (off < ageEnd && off >= 0) {
					fieldIn.skipTo(off);
					if(!ageFilter.accept(fieldIn.next()))
						bs.clear(off);
	                off = bs.nextSetBit(off+1);	                    
				}
			}
			
//			if (bs.cardinality() == 0)
//				return;
		}	
	}
	
	public boolean isUserActiveCublet() {
		return this.bUserActiveCublet;
	}

	public boolean isAgeActiveCublet() {
		return this.bAgeActiveCublet;
	}
	
	public boolean isUserActiveChunk() {
		return this.bUserActiveChunk;
	}
	
	public boolean isAgeActiveChunk() {
		return this.bAgeActiveChunk;
	}

}
