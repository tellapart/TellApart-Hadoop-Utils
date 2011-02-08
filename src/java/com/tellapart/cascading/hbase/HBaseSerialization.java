package com.tellapart.cascading.hbase;

import java.io.ObjectInputStream;
import java.io.IOException;
import java.io.Serializable;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.JobConf;

import cascading.CascadingException;

// Using the old hadoop API until Cascading supports the newer one.
@SuppressWarnings("deprecation")

/**
 * HBaseSerialization is a wrapper around the Apache Hadoop SerializationFactory
 * and allows us to serialize and deserialize objects using Hadoop Serialization into
 * HBase.
 * In addition to the base Hadoop Serialization, HBaseSerialization also supports
 * serializing primitives (Integer, Long, Double, Boolean, String, byte[], etc.).
 * Serializing primitives is done by converting them to a Hadoop Writable, and
 * serializing that object. This is transparent to the end user.
 * HBaseSerialization must be serializable since Cascading serializes and unserializes
 * the FullHBaseScheme that uses it when running on a cluster.
 * SerializationFactory is not serializable, so to save the state we save the ioSerializations
 * hadoop property that's used to find Serialization classes that we support.
 */
public class HBaseSerialization implements Serializable {
  private static final long serialVersionUID = 1L;
  private transient SerializationFactory mFactory = null;

  private String ioSerializations = null;

  /**
   * Create an HBaseSerialization using a JobConf that must have io.serializations
   * filled in to the list of Hadoop serializations we will support.
   * @param conf the JobConf with the settings we'll use.
   */
  public HBaseSerialization(JobConf conf) {
    ioSerializations = conf.get("io.serializations");
    initialize();
  }

  /**
   * Initialize the SerializationFactory used to access Hadoop serializers.
   */
  private void initialize() {
    JobConf conf = new JobConf();
    conf.set("io.serializations", ioSerializations);
    mFactory = new SerializationFactory(conf);
  }

  /**
   * @return a Serializer for the given class.
   * @throws a CascadingException if the Serializer for that particular class
   * could not be loaded.
   */
  private Serializer<?> getNewSerializer(Class<?> type ) {
    try {
      return mFactory.getSerializer(type);
    } catch( NullPointerException exception ) {
      throw new CascadingException("Unable to load serializer for: " + type.getName()
          + " from: " + mFactory.getClass().getName() );
    }
  }

  /**
   * @return a Deserializer for the given class.
   * @throws a CascadingException if the Deserializer for that particular class
   * could not be loaded.
   */
  private Deserializer<?> getNewDeserializer(Class<?> type ) {
    try {
      return mFactory.getDeserializer(type);
    } catch( NullPointerException exception ) {
      throw new CascadingException("Unable to load deserializer for: " + type.getName()
          + " from: " + mFactory.getClass().getName() );
    }
  }

  /**
   * Serializes the given object into a byte array.
   * This supports serialization for bytes, Strings, and any object that is handled
   * by an Apache Hadoop serialization.
   * @param o the Object to serialize
   * @return byte[] array of the serialized object.
   *         If o is null, returns null.
   */
  public byte[] serialize(Object o) {
    if (o == null) {
      return null;
    }
    if (o instanceof String) {
      return ((String)o).getBytes();
    } else if (o instanceof byte[]) {
      return (byte[])o;
    } else {
      // Try to convert the object to a Writable,
      // and serialize the result of that, if we have a writable.
      Writable oWritable = toWritable(o);
      if (oWritable == null) {
        return internalSerialize(o);
      } else {
        return internalSerialize(oWritable);
      }
    }
  }

  /**
   * Converts primitives (Integer, Long, Double, Boolean) to a Writable
   * for writing them, since the serialization schemes we know about don't
   * support serializing these types by themselves.
   * @param o the object to serialize
   * @return a Writable wrapping this object, or null if we don't know how
   * to convert this object to a writable.
   */
  private Writable toWritable(Object o) {
    if (o instanceof Boolean) {
      return new BooleanWritable((Boolean)o);
    } else if (o instanceof Integer) {
      return new IntWritable((Integer)o);
    } else if (o instanceof Long) {
      return new LongWritable((Long)o);
    } else if (o instanceof Double) {
      return new DoubleWritable((Double)o);
    }
    return null;
  }

  /**
   * Converts Writables of primitives (Boolean, Int, Long, Double) to their
   * associated primitive.
   * This is used to read objects that were written as Writables but should
   * be returned to the user as the primitive value inside the Writable.
   * @param o the object to serialize
   * @return a Writable wrapping this object, or null if we don't know how
   * to convert this object to a writable.
   */
  private Object fromWritable(Object o) {
    if (o instanceof BooleanWritable) {
      return ((BooleanWritable)o).get();
    } else if (o instanceof IntWritable) {
      return ((IntWritable)o).get();
    } else if (o instanceof LongWritable) {
      return ((LongWritable)o).get();
    } else if (o instanceof DoubleWritable) {
      return ((DoubleWritable)o).get();
    }
    return null;
  }

  /**
   * @return the Writable version of a class. The user of this class can then specify
   * that a given column is of Boolean or Long type, and this will return the correct
   * Writable object that was written to HBase and that we should use for deserializing
   * the class.
   */
  private Class<?> getWritableVersionOfClass(Class<?> clazz) {
    if (clazz.equals(Boolean.class)) {
      return BooleanWritable.class;
    } else if (clazz.equals(Integer.class)) {
      return IntWritable.class;
    } else if (clazz.equals(Long.class)) {
      return LongWritable.class;
    } else if (clazz.equals(Double.class)) {
      return DoubleWritable.class;
    }
    return null;
  }

  /**
   * Serializes o by getting a Hadoop Serializer based of the object's class.
   * @param o object to serialize
   * @return the serialized byte[] array.
   */
  private byte[] internalSerialize(Object o) {
    Serializer serializer = getNewSerializer(o.getClass());

    // Write the object into an in-memory byte array.
    ByteArrayOutputStream byteOs = new ByteArrayOutputStream();
    try {
      serializer.open(byteOs);
      serializer.serialize(o);
      serializer.close();
    } catch (IOException ex) {
      // This should never happen since we're writing to an in-memory ByteArrayOutputStream.
      throw new RuntimeException(ex);
    }
    return byteOs.toByteArray();
  }

  /**
   * Deserialize a byte array into an object based on the given class.
   * @param bytes the bytes to deserialize.
   * @param clazz the expected class these bytes represent.
   * @return the deserialized Object.
   */
  public Object deserialize(byte[] bytes, Class<?> clazz) {
    try {
      // If null, return null.
      if (bytes == null) {
        return null;
      }

      // First check for our special handling byte[] and Strings.
      if (clazz.equals(byte[].class)) {
        return bytes;
      } else if (clazz.equals(String.class)) {
        return new String(bytes);
      } else {
        Class<?> writableVersionOfClazz = getWritableVersionOfClass(clazz);
        if (writableVersionOfClazz != null) {
          clazz = writableVersionOfClazz;
        }

        // Now deserialize using the hadoop deserializer.
        Deserializer deserializer = getNewDeserializer(clazz);
        deserializer.open(new ByteArrayInputStream(bytes));
        Object o = clazz.newInstance();
        o = deserializer.deserialize(o);
        deserializer.close();

        if (writableVersionOfClazz != null) {
          return fromWritable(o);
        } else {
          return o;
        }
      }
    } catch(Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  // We add a hook to the deserialization to also re-initialize the SerializationFactory class.
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    initialize();
  }
}
