package com.tellapart.thrift.hadoopserialization;

import java.io.InputStream;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

/**
 * Custom Thrift record deserializer, enabling Thrift records to be passed
 * as keys and values in Hadoop jobs.  Note that this class depends on
 * homogeneous serialized data - that is, all serialized records are of the
 * same subclass of TBase.  Package private: clients should use
 * ThriftHadoopSerialization public interface.
 */
class ThriftHadoopDeserializer implements Deserializer<TBase> {
  private TTransport mTransport;
  private Class<? extends TBase> mClass;

  /**
   * C-tor
   * @param c The TBase subclass type to be instantiated from serialized data.
   */
  public ThriftHadoopDeserializer(Class<? extends TBase> c) {
    mClass = c;
  }

  @Override
  public void open(InputStream is) {
    mTransport = new TIOStreamTransport(is);
  }

  @Override
  public TBase deserialize(TBase thriftRecord) {
    try {
      if (thriftRecord == null) {
        // If we're not passed an instance to populate, allocate a new object
        // of the appropriate TBase subtype.
        thriftRecord = mClass.newInstance();
      }
      TProtocol protocol = new TBinaryProtocol(mTransport);
      thriftRecord.read(protocol);
      return thriftRecord;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    mTransport.close();
  }
}
