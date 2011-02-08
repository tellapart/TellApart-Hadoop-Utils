package com.tellapart.thrift.hadoopserialization;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.thrift.TBase;

import cascading.tuple.hadoop.SerializationToken;

/**
 * Class to enable hadoop to utilize Thrift records as keys and values by
 * instantiating custom a serializer and deserializer.  This class must be added
 * to the hadoop-site.xml config by appending the classname to the
 * io.serializations property.
 *
 * All Thrift buffers we expect to serialize should be added to the list of SerializationToken
 * annotations below. We start this list with token id 1000 since ids less than 128 are reserved
 * for Cascading use, and to give us headroom.
 */
@SerializationToken(
  tokens = {1000, 1001, 1002},
  classNames = {"com.tellapart.serverlog.ServerLogEntry",
                "com.tellapart.crumb.CrumbActionDetails",
                "com.tellapart.reporting.Conversion"})
public class ThriftHadoopSerialization implements Serialization<TBase> {

  @Override
  public boolean accept(Class<?> c) {
    return TBase.class.isAssignableFrom(c);
  }

  @Override
  public Serializer<TBase> getSerializer(Class<TBase> c) {
    return new ThriftHadoopSerializer();
  }

  @Override
  public Deserializer<TBase> getDeserializer(Class<TBase> c) {
    return new ThriftHadoopDeserializer(c);
  }
}
