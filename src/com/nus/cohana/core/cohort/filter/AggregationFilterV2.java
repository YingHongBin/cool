package com.nus.cohana.core.cohort.filter;

import java.util.List;

// import org.apache.hadoop.hdfs.server.namenode.snapshot.TestAclWithSnapshot;

import com.nus.cohana.core.cohort.filter.ExtendedFieldSet.FieldValueType;
import com.nus.cohana.core.io.readstore.FieldRS;
import com.nus.cohana.core.io.readstore.MetaFieldRS;

import static com.google.common.base.Preconditions.checkArgument;

public class AggregationFilterV2 implements FieldFilter {

	private double[] minValues;
	
	private double[] maxValues;
	
	private double[] updatedMinValues;
	
	private double[] updatedMaxValues;
	
	private ExtendedFieldSet fieldSet;

    boolean updated = true;

    public AggregationFilterV2(ExtendedFieldSet set, List<String> values) {
		checkArgument(values != null && values.isEmpty() == false);
		this.fieldSet = set;
        
        this.minValues = new double[values.size()];
        this.maxValues = new double[values.size()];
        this.updatedMaxValues = new double[values.size()];
        this.updatedMinValues = new double[values.size()];
        for (int i = 0; i < values.size(); i++) {
            String[] range = values.get(i).split("\\|");
            this.minValues[i] = Double.parseDouble(range[0]);
            this.maxValues[i] = Double.parseDouble(range[1]);
        }
    }

    @Override
    public void updateValues(Double v) {
    	
		FieldValueType valueType = fieldSet.getFieldValue().getType();
		
		if (valueType == FieldValueType.AbsoluteValue)
			return;
    	
    	if (v == null) {
    		updated = false;
    		return;
    	}
    	
        switch (valueType) {
            case IncreaseByAbsoluteValue:
                for (int i = 0; i < minValues.length; i++) {
                    updatedMinValues[i] = v + minValues[i];
                    updatedMaxValues[i] = v + maxValues[i];
                }
                break;
            case IncreaseByPercentage:
                for (int i = 0; i < minValues.length; i++) {
                    if (v == 0.0) {
                        updatedMinValues[i] = minValues[i] < 0 ? -Double.MAX_VALUE : 0;
                        updatedMaxValues[i] = maxValues[i] > 0 ? Double.MAX_VALUE : 0;
                    } else {
                        updatedMinValues[i] = v * (1 + minValues[i]/100);
                        updatedMaxValues[i] = v * (1 + maxValues[i]/100);
                    }
                }
                break;
            default:
                break;
        }
    }   


    @Override
	public boolean accept(Double v) {
        if (v == null) return false;
        boolean r = !updated;
        int i = 0;
        while (!r && i < minValues.length) {
            r = r || (v >= updatedMinValues[i] && v <= updatedMaxValues[i]);
            i++;
        }
        return r;
	} 

	@Override
	public double getMinKey() {
		throw new UnsupportedOperationException();
		}

	@Override
	public double getMaxKey() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean accept(MetaFieldRS metaField) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean accept(FieldRS chunkField) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean accept(int v) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int nextAcceptTuple(int start, int to) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ExtendedFieldSet getFieldSet() {
		// TODO Auto-generated method stub
		return this.fieldSet;
	}

	@Override
    public List<String> getValues() {
        throw new UnsupportedOperationException();
    }
}
