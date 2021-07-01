/**
 * 
 */
package com.nus.cohana.core.cohort.filter;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;

import com.nus.cohana.core.cohort.converter.NumericConverter;
import com.nus.cohana.core.cohort.filter.ExtendedFieldSet.FieldValueType;
import com.nus.cohana.core.cohort.schema.DataType;
import com.nus.cohana.core.cohort.schema.FieldSchema;
import com.nus.cohana.core.io.InputVector;
import com.nus.cohana.core.io.readstore.FieldRS;
import com.nus.cohana.core.io.readstore.MetaFieldRS;

/**
 * @author david, qingchao
 *
 */
public class RangeFieldFilter implements FieldFilter {

	private double[] minValues;
	
	private double[] maxValues;
	
	private double[] updatedMinValues;
	
	private double[] updatedMaxValues;

    private double minimum = Double.MAX_VALUE;

    private double maximum = Double.MIN_VALUE;
	
	private InputVector values;
	
	private ExtendedFieldSet fieldSet;

    boolean updated = true;

    FieldSchema schema; 

	public RangeFieldFilter(FieldSchema schema, ExtendedFieldSet set, List<String> values, NumericConverter conv) {
		checkArgument(values != null && values.isEmpty() == false);
		this.fieldSet = checkNotNull(set);
        this.minValues = new double[values.size()];
        this.maxValues = new double[values.size()];
        this.updatedMaxValues = new double[values.size()];
        this.updatedMinValues = new double[values.size()];
        this.schema = schema;

		FieldValueType valueType = fieldSet.getFieldValue().getType();
        for (int i = 0; i < values.size(); i++) {
            String[] range = values.get(i).split("\\|");
            if (valueType == FieldValueType.AbsoluteValue){
                // If the value is absolute number
                this.minValues[i] = conv.toDouble(range[0]);
                this.maxValues[i] = conv.toDouble(range[1]);
            } else {
                // If the value is percentage
                this.minValues[i] = Double.parseDouble(range[0]);
                this.maxValues[i] = Double.parseDouble(range[1]);
            }
            this.updatedMaxValues[i] = maxValues[i];
            this.updatedMinValues[i] = minValues[i];
            minimum = minimum > minValues[i] ? minValues[i] : minimum;
            maximum = maximum < maxValues[i] ? maxValues[i] : maximum;
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
	public double getMinKey() {
		return minimum;
	}

	@Override
	public double getMaxKey() {
		return maximum;
	}

	@Override
	public boolean accept(MetaFieldRS metaField) {
	    // TODO: FieldValueType is purposeless?
        return fieldSet.getFieldValue().getType() != FieldValueType.AbsoluteValue ||
            this.schema.getDataType() == DataType.Aggregate ||
            !(metaField.getMinValue() > maximum || metaField.getMaxValue() < minimum);
	}

	@Override
	public boolean accept(FieldRS chunkField) {
		int min = chunkField.minKey();
		int max = chunkField.maxKey();
		this.values = chunkField.getValueVector();
		return fieldSet.getFieldValue().getType() != FieldValueType.AbsoluteValue ||
            this.schema.getDataType() == DataType.Aggregate ||
            !(min > maximum || max < minimum);
	}

	@Override
	public boolean accept(int v) {
        boolean r = !updated;
        int i = 0;
        while (!r && i < minValues.length) {
           r = r || (v >= updatedMinValues[i] && v <= updatedMaxValues[i]); 
           i++;
        }
        return r;
	}
	
	@Override
	public int nextAcceptTuple(int start, int to) {		
		values.skipTo(start);		
		while(start < to && !accept(values.next())) ++start;
		return start;
	}

	@Override
	public boolean accept(Double v) {
        boolean r = !updated;
        int i = 0;
        while (!r && i < minValues.length) {
           r = r || (v >= updatedMinValues[i] && v <= updatedMaxValues[i]); 
           i++;
        }
        return r;
	}

	@Override
	public ExtendedFieldSet getFieldSet() {
		return this.fieldSet;
	}

	@Override
    public List<String> getValues() {
	    List<String> values = new ArrayList<>();
	    for (int i = 0; i < minValues.length; i++) {
	        values.add(minValues[i] + "|" + maxValues[i]);
        }
	    return values;
    }
}
