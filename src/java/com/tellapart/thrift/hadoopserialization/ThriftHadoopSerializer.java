package com.tellapart.thrift.hadoopserialization;

import java.io.OutputStream;

import org.apache.hadoop.io.serializer.Serializer;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

/**
 * Custom Thrift record serializer, enabling Thrift records to be passed
 * as keys and values in Hadoop jobs.  Package private: clients should use
 * ThriftHadoopSerialization public interface.
 */
class ThriftHadoopSerializer implements Serializer<TBase> {
  private TTransport mTransport;

  @Override
  public void open(OutputStream os) {
    mTransport = new TIOStreamTransport(os);
  }

  @Override
  public void serialize(TBase rpv) {
    TProtocol protocol = new TBinaryProtocol(mTransport);
    try {
      rpv.write(protocol);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    mTransport.close();
  }
}
