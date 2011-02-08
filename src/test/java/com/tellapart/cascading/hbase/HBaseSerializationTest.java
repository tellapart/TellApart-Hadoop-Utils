package com.tellapart.cascading.hbase;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.tellapart.test.thrift.TestStruct;

import com.tellapart.cascading.hbase.HBaseSerialization;

public class HBaseSerializationTest {
  HBaseSerialization mSerialization;

  public static final String TEST_SERIALIZATIONS =
        "org.apache.hadoop.io.serializer.WritableSerialization,"
        + "cascading.tuple.hadoop.TupleSerialization,"
        + "com.tellapart.thrift.hadoopserialization.ThriftHadoopSerialization";

  @Before
  public void setup() {
    JobConf conf = new JobConf();
    conf.set("io.serializations", TEST_SERIALIZATIONS);
    mSerialization = new HBaseSerialization(conf);
  }

  @Test
  public void testSerializeString() {
    String s = "abcdefg";

    serializeAndAssert(s);
  }


  @Test
  public void testSerializeThrift() throws Exception {
    TestStruct struct = new TestStruct();
    struct.setIntField(23);
    struct.setLongField(145l);
    struct.setStringField("asdf");
    struct.setListField(new ArrayList<String>());
    struct.getListField().add("a");
    struct.getListField().add("b");
    struct.setMapOfLists(new HashMap<String, List<Integer>>());
    struct.getMapOfLists().put("key2", new ArrayList<Integer>());
    struct.getMapOfLists().get("key2").add(1);
    struct.getMapOfLists().get("key2").add(2);
    serializeAndAssert(struct);
  }

  @Test
  public void testSerializeBytes() throws Exception {
    byte[] bytes = new byte[5];
    bytes[0] = -128;
    bytes[1] = 0;
    bytes[2] = 23;
    bytes[3] = 98;
    bytes[4] = 127;

    serializeAndAssert(bytes);
  }

  @Test
  public void testSerializeWritables() throws Exception {
    MapWritable map = new MapWritable();
    map.put(new Text("aerger"), new LongWritable(234));

    byte[] serialized = mSerialization.serialize(map);
    MapWritable deserialized = (MapWritable) mSerialization.deserialize(serialized,
        MapWritable.class);

    // MapWritable doesn't properly implement equals(), so we make a copy of it into
    // a HashMap to do the assertEquals.
    Map<Writable, Writable> actual = new HashMap<Writable, Writable>(deserialized);
    Map<Writable, Writable> expected = new HashMap<Writable, Writable>(map);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSerializeNull() throws Exception {
    Assert.assertNull(mSerialization.serialize(null));
    Assert.assertEquals(null, mSerialization.deserialize(null, null));
    Assert.assertEquals(0, ((byte[])mSerialization.deserialize(new byte[0], byte[].class)).length);
  }

  @Test
  public void testSerializePrimitives() throws Exception {
    serializeAndAssert(3);
    serializeAndAssert(10023023l);
    serializeAndAssert(100.456);
    serializeAndAssert(true);
    serializeAndAssert(false);
  }

  /**
   * Serialize the object using HBaseSerialization and assert that the deserialized
   * version is equal to the original object.
   * @param orig the object to test.
   */
  private void serializeAndAssert(Object orig) {
    byte[] serialized = mSerialization.serialize(orig);
    Object deserialized = mSerialization.deserialize(serialized, orig.getClass());
    Assert.assertEquals(orig, deserialized);
  }
}
