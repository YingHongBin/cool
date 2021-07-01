/**
 * 
 */
package com.nus.cohana.core.cohort.schema;

/**
 * For OLAP We use Segment, Property & Metric
 * 
 * For cohort, we use all field types
 * 
 * ChangeList:
 * 1. Change the following field type to be compatible with SIGMOD paper. 
 * EventDay -> ActionTime
 * Event -> Action
 * 
 * @author david
 *
 */
public enum FieldType {

  AppKey,

  UserKey,

  ActionTime,

  Day,

  Action,

  Segment,

  Property,

  Metric, 

  Week,

  Month,

  Birth;

  public static FieldType fromInteger(int i) {
    switch (i) {
      case 0:
        return AppKey;
      case 1:
        return UserKey;
      case 2:
        return ActionTime; //Numeric
      case 3:
        return Day; //Numeric
      case 4:
        return Action;
      case 5:
        return Segment;
      case 6:
        return Property;
      case 7:
        return Metric; //Numeric
      case 8:
        return Week; //Numeric
      case 9:
        return Month; // Numeric
      default:
        throw new IllegalArgumentException("FieldType: " + i);
    }
  }

}
