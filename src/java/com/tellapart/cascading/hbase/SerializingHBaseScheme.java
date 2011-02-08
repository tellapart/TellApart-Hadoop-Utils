package com.tellapart.cascading.hbase;

import java.io.IOException;

import cascading.CascadingException;

import cascading.hbase.HBaseScheme;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.mapred.JobConf;

// Yes, we're using the old Hadoop APIs until Cascading is ready for the new ones.
// Sigh.
@SuppressWarnings("deprecation")

/**
 * The SerializingHBaseScheme class is a {@link HBaseScheme} subclass.
 * It is used in conjunction with the {@HBaseTap} to allow for the reading and writing of data to
 * and from a HBase cluster.
 * Unlike HBaseScheme, it supports serialization of the types inside the Tuples, as well as
 * returning the timestamp of the HBase cell that was read.
 *
 * This must extend HBaseScheme since HBaseTap only takes an HBaseScheme.
 * The other option is to make our own HBaseTap, or modify HBaseScheme directly.
 *
 * @see HBaseTap
 */
public class SerializingHBaseScheme extends HBaseScheme {
  private static final long serialVersionUID = 1L;

  // Indicates if the scheme is used for reading or writing.
  // This is a bit of a hack: Cascading doesn't allow a tap equal to another tap
  // to be used both as a source and a sink for a Flow.
  // For HBaseTaps, this is sometimes desirable -- if we want to read from a column
  // in HBase, do some transformation, and write back to the same column. In such
  // a case, normally, the HBaseTaps and schemes are equal so Cascading throws an error.
  // To overcome this, we can mark one of the schemes for read and the other for write,
  // and make sure (in the equals() method), that this is used for distinguishing between
  // these kinds of taps. That way we can tell Cascading that it's OK to both read and
  // write from this same source.
  public enum Direction {
    FOR_READ,
    FOR_WRITE,
  }

  // Serialization object used to serialize and deserialize values.
  private HBaseSerialization mSerialization;

  // Indicates if the scheme will return timestamped results.
  private boolean mIsTimestamped = false;

  // The Direction of this scheme (READ or WRITE).
  private Direction mDirection;

  // Indicates which classes we expect the HBase table to contain.
  private Class<?>[] mClasses;

  // Corresponds to what we should set hbase.client.scanner.caching for this scheme.
  // If not set, we do not override the jobconf value.
  private Integer mClientScanCaching = null;

  /**
   * Constructor SerializingHBaseScheme creates a new SerializingHBaseScheme instance using fully
   * qualified column names. This means that the names of the fields in valueFields should
   * correspond to the column family names in hbase that we will be pulling our data from.
   *
   * @param keyField    the Field that will be used for the row key.
   * @param valueFields the Fields, from the input pipe into this scheme, that will be written
   *                    out to HBase. Each Field must be an HBase column.
   * @param valueClasses the Classes that correspond to the values inside each of the valueFields.
   *                     When this is used as a source, these classes are used to determine
   *                     how to deserialize the object corresponding to that field.
   * @param isTimestamped When this is used as a source, determines whether to return the raw
   *                      deserialized object in the output tuple, or the CellElement
   *                      wrapper object which will carry the deserialized object as well as the
   *                      HBase timestamp of the cell it came from.
   * @param use The Direction (FOR_READ or FOR_WRITE) of this scheme.
   */
  public SerializingHBaseScheme(Fields keyField, Fields valueFields, Class<?>[] valueClasses,
        boolean isTimestamped, Direction direction) {
    super(keyField, valueFields);
    validateClasses(valueFields, valueClasses);
    mClasses = valueClasses;
    mIsTimestamped = isTimestamped;
    mDirection = direction;
  }

  public SerializingHBaseScheme(Fields keyField, String familyName, Fields valueFields,
      Class<?>[] valueClasses, boolean isTimestamped, Direction direction) {
    super(keyField, familyName, valueFields);
    validateClasses(valueFields, valueClasses);
    mClasses = valueClasses;
    mIsTimestamped = isTimestamped;
    mDirection = direction;
  }

  /**
   * Sets the hbase.client.scanner.caching property for this scheme.
   * This determines how many rows are cached for HBase reads.
   * If not set, this defers to the hbase.client.scanner.caching set in the job conf
   * that invoked this scheme, or to the default of "10".
   * @param clientScanCaching how many rows to cache.
   */
  public void setClientScanCaching(int clientScanCaching) {
    mClientScanCaching = clientScanCaching;
  }

  /**
   * Validates that the number of classes passed in matches the number of fields.
   */
  private void validateClasses(Fields valueFields, Class<?>[] valueClasses) {
    if (valueClasses.length != valueFields.size()) {
      throw new CascadingException("Expected same number of classes and value fields for "
          + "SerializingHBaseScheme. "
          + "Got: " + valueClasses.length + " classes, and " + valueFields.size() + " fields.");
    }
  }

  @Override
  public void sinkInit(Tap tap, JobConf conf) throws IOException {
    super.sinkInit(tap, conf);
    // We can only initialize serialization here and not in the constructor since we don't have
    // JobConf available to us in the constructor.
    initSerialization(conf);
  }

  @Override
  public void sourceInit(Tap tap, JobConf conf) throws IOException {
    super.sourceInit(tap, conf);
    // We can only initialize serialization here and not in the constructor since we don't have
    // JobConf available to us in the constructor.
    initSerialization(conf);
    if (mClientScanCaching != null) {
      conf.set("hbase.client.scanner.caching", mClientScanCaching.toString());
    }
  }

  /**
   * Initialize the HBaseSerialization used to serialize and deserialize classes.
   */
  private void initSerialization(JobConf conf) {
    mSerialization = new HBaseSerialization(conf);
  }

  @Override
  protected byte[] toCell(Tuple tuple, int pos) {
    return toCell(tuple.getObject(pos));
  }

  /**
   * Serializes the given object using HBaseSerialization.
   * @param object object to serialize.
   * @return byte[] serialized bytes of the object.
   */
  protected byte[] toCell(Object object) {
    return mSerialization.serialize(object);
  }

  @Override
  protected Object fromCell(Cell cell, int fieldIndex) {
    if (cell == null) {
      return null;
    }

    // Deserialize this element based on the Class we expect this column to contain.
    Object element = mSerialization.deserialize(cell.getValue(), mClasses[fieldIndex]);
    // Wrap the element in a CellElement if requested.
    if (mIsTimestamped && element != null) {
      return new CellElement(element, cell.getTimestamp());
    } else {
      return element;
    }
  }

  public boolean equals(Object other) {
    boolean isEqual = super.equals(other);
    if (isEqual) {
      SerializingHBaseScheme otherScheme = (SerializingHBaseScheme)other;
      isEqual &= (otherScheme.mIsTimestamped == mIsTimestamped);
      isEqual &= (otherScheme.mDirection == mDirection);
    }
    return isEqual;
  }

  public String toString() {
    return super.toString() + "," + mIsTimestamped + "," + mDirection;
  }
}
