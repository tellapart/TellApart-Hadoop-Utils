package com.tellapart.cascading.hbase;

import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.mapred.JobConf;

import com.tellapart.test.cascading.ClusterTestHelper;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;

import cascading.hbase.HBaseTap;
import cascading.hbase.HBaseTestCase;

import cascading.operation.regex.RegexSplitter;

import cascading.pipe.Each;
import cascading.pipe.Pipe;

import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class SerializingHBaseSchemeTest extends HBaseTestCase {
  public ClusterTestHelper mHelper;

  private static final Map<Object, Object> PROPERTIES = new HashMap<Object, Object>();
  { PROPERTIES.put("io.serializations", HBaseSerializationTest.TEST_SERIALIZATIONS);
  }

  public SerializingHBaseSchemeTest() {
    // Start a test with one region server and no DFS.
    super(1, false);
  }

  public void setUp() throws Exception {
    super.setUp();
    mHelper = new ClusterTestHelper(PROPERTIES);
  }

  public void tearDown() throws Exception {
    mHelper.tearDown();
  }

  public void testSimpleFlow() throws Exception {
    // This tests reading from a file on HFS and writing the output tuples to HBase.
    // It makes sure that the tuples that result are serialized and deserialized properly.
    Fields inputFields = new Fields("num", "lower", "upper");
    TupleEntryCollector input = mHelper.makeCollectorForWrite("input", inputFields);

    // Set up the input.
    Tuple[] expected = new Tuple[] {
      new Tuple("1", "a", "b"),
      new Tuple("2", "test", "other"),
    };

    for (Tuple t : expected) {
      input.add(t);
    }
    input.close();

    // Create flow to read from local file and insert into HBase.
    Tap source = new Hfs(new SequenceFile(inputFields), mHelper.manageTemporaryPath("input"));

    Pipe pipe = new Pipe("values");
    Fields keyFields = new Fields("num");
    Fields valueFields = new Fields("lower", "upper");
    Tap hBaseTap = new HBaseTap("testTable",
        new SerializingHBaseScheme(keyFields, valueFields,
                                       new Class<?>[]{String.class, String.class},
          false, SerializingHBaseScheme.Direction.FOR_WRITE),
        SinkMode.REPLACE);

    Flow flow = new FlowConnector(PROPERTIES).connect(source, hBaseTap, pipe);
    flow.complete();

    mHelper.expectResult(flow, expected);
  }
}
