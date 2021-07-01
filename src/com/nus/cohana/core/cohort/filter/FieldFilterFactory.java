/**
 * 
 */
package com.nus.cohana.core.cohort.filter;

import java.util.List;

import com.nus.cohana.core.cohort.converter.DayIntConverter;
import com.nus.cohana.core.cohort.converter.StringIntConverter;
import com.nus.cohana.core.cohort.converter.WeekIntConverter;
import com.nus.cohana.core.cohort.filter.ExtendedFieldSet.FieldValue;
import com.nus.cohana.core.cohort.filter.ExtendedFieldSet.FieldValueType;
import com.nus.cohana.core.cohort.schema.FieldSchema;

/**
 * @author david, xiezl
 *
 */
public class FieldFilterFactory {

    public FieldFilter create(FieldSchema schema, ExtendedFieldSet fieldSet, List<String> values) {
        switch (schema.getFieldType()) {
        case AppKey:
        case Segment:
        case Action:
        case UserKey:
            return new SetFieldFilter(fieldSet, values, null);
        case ActionTime: {
            // The following "if" is added in order to get compatible with old codes
            if (fieldSet == null) {
                ExtendedFieldSet fs = new ExtendedFieldSet();
                FieldValue v = new FieldValue();
                v.setType(FieldValueType.AbsoluteValue);
                fs.setFieldValue(v);
                return new RangeFieldFilter(schema, fs, values, new DayIntConverter());
            }
            return new RangeFieldFilter(schema, fieldSet, values, new DayIntConverter());
        }
        case Day:
            return new RangeFieldFilter(schema, fieldSet, values, new DayIntConverter());
        case Week:
            return new RangeFieldFilter(schema, fieldSet, values, new WeekIntConverter());
        case Month:
            return new RangeFieldFilter(schema, fieldSet, values, null);
        case Metric:
            return new RangeFieldFilter(schema, fieldSet, values, new StringIntConverter());
        case Birth:
            return new AggregationFilterV2(fieldSet, values);
        // return this.addWorker(schema, fieldSet, values)
        default:
            throw new IllegalArgumentException("Unknown FieldFilter: " + schema.getFieldType());
        }
    }

}
