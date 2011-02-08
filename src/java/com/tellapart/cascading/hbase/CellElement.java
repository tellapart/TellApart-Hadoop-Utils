package com.tellapart.cascading.hbase;

import cascading.tuple.TupleEntry;

/**
 * A CellElement represents a Cell from an HBase table intended
 * to be stored in a Cascading Tuple.
 * The CellElement supports storing the value and timestamp of an
 * HBase cell.
 */
public class CellElement<T> {
  private T mObject;
  private long mTimestamp;

  /**
   * Create the CellElement
   * @param object - the object desieralized from HBase.
   * @param timestamp - timestamp from hbase, in microseconds.
   */
  public CellElement(T object, long timestamp) {
    mObject = object;
    mTimestamp = timestamp;
  }

  /**
   * @return the timestamp of this object from HBase, in microseconds.
   */
  public long getTimestamp() {
    return mTimestamp;
  }

  /**
   * Get the object stored in thet cell.
   * @return the object.
   */
  public T getObject() {
    return mObject;
  }

  /**
   * Get the object inside the CellElement stored in the given field in a TupleEntry.
   * @param entry the TupleEntry the object is stored in.
   * @param field the name of the field we expect to find a CellElement at.
   * @return the object inside the CellElement
   */
  public static Object get(TupleEntry entry, String field) {
    CellElement<?> cell = (CellElement<?>) entry.get(field);
    if (cell == null) { return null; }
    return cell.getObject();
  }

  /**
   * Get the timestamp of the CellElement stored in the given field in a TupleEntry.
   * @param entry the TupleEntry the object is stored in.
   * @param field the name of the field we expect to find a CellElement at.
   * @return the timestamp of the CellElement at field 'field'.
   */
  public static Long getTimestamp(TupleEntry entry, String field) {
    CellElement<?> cell = (CellElement<?>) entry.get(field);
    if (cell == null) { return null; }
    return cell.getTimestamp();
  }

  @Override
  public String toString() {
    return getObject().toString() + " @time=" + getTimestamp();
  }
}
