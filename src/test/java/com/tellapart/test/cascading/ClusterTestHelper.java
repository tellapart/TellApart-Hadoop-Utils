package com.tellapart.test.cascading;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.lang.ArrayUtils;

import org.apache.hadoop.mapred.JobConf;

import cascading.CascadingException;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopUtil;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 * ClusterTestHelper manages temporary paths and runs cascading flows in unittests that run
 * an entire cascading flow.
 * Sample usage:
 *   mHelper = new ClusterTestHelper(getProperties());
 *   ...
 *   collector = mHelper.makeCollectorForWrite(path, fields);
 *   collector.add(my_test_input);
 *   ...
 *   collector.close();
 *   ...
 *   mHelper.runFlow(...);
 *   mHelper.expectResult(...);
 *   ...
 *   mHelper.tearDown();
 */
public class ClusterTestHelper {
  // Paths which are created by the inputs and outputs to cluster unittests.
  // These paths will be deleted when the test case is tore down.
  private List<String> mPathsToManage;
  private FlowConnector mFlowConnector;

  /**
   * Create a ClusterTestHelper that runs all flows with the base properties passed in.
   * @param properties - map of job configuration properties to run with.
   * Usually this is obtained from the cascading ClusterTestCase's getProperties() method.
   */
  public ClusterTestHelper(Map<Object, Object> properties) throws IOException {
    mPathsToManage = new ArrayList<String>();
    mFlowConnector = new FlowConnector(properties);
  }

  public JobConf getJobConf() {
    return HadoopUtil.createJobConf(mFlowConnector.getProperties(), null);
  }

  /**
   * Tear down the ClusterTestHelper and delete the temporary paths it manages.
   * You must call this at the tearDown method of any test case which uses this class.
   * @throws IOException if there's an error deleting the directories.
   */
  public void tearDown() throws IOException {
    for (String path : mPathsToManage) {
      deleteDirectory(path);
    }
  }

  /**
   * Creates a hadoop input directory on the path inputPath, with
   * a SequenceFile scheme with the given fields.
   * Returns a TupleEntryCollector that can be used to write to this path.
   * The user of the method must call collector.close() after writing to close
   * the input files.
   *
   * @param path A folder into which the output will be written.
   * This will create a folder under the java temporary file directory.
   * When this testcase is teared down, this folder will be deleted.
   * @param fields The fields of the files.
   * @return a TupleEntryCollector that can be used to write into this folder.
   */
  public TupleEntryCollector makeCollectorForWrite(String path, Fields fields)
      throws IOException, CascadingException {
    String inputPath = manageTemporaryPath(path);

    File inputFile = new File(inputPath);
    if (inputFile.exists()) {
      throw new CascadingException("Input file " + inputPath + " already exists.");
    }
    Tap inputTap = new Hfs(new SequenceFile(fields), inputPath, SinkMode.REPLACE);
    TupleEntryCollector collector = inputTap.openForWrite(getJobConf());
    return collector;
  }

  /**
   * Create and run a cascading flow that connects the given pipe to an input
   * with the given fields and an output with the given fields.
   * Manages the input and output to delete them when the test case is done.
   * @param tail Pipe to run.
   * @param inputFields input fields of the input.
   * @param input the input name.
   * @param outputFields input fields of the input.
   * @param output the output name.
   * @return Flow the flow, after it was started and completed.
   */
  public Flow runFlow(Pipe tail, Fields inputFields, String input,
      Fields outputFields, String output) {
    Map<String, Fields> inputMap = ArrayUtils.toMap(new Object[][] {
      {input, inputFields},
    });
    return runFlow(tail, inputMap, outputFields, output);
  }

  /**
   * Create and run a cascading flow that connects the given pipe to several inputs.
   * Manages the input and output to delete them when the test case is done.
   * @param tail pipe to run
   * @param inputs a map from input names to the Fields they represent.
   * @param outputFields the fields of the output.
   * @param output the output name.
   * @return a completed flow.
   */
  public Flow runFlow(Pipe tail, Map<String, Fields> inputs,
      Fields outputFields, String output) {
    return runFlow(new Pipe[]{tail}, inputs, new Fields[] {outputFields});
  }

  /**
   * Create and run a cascading flow that connects the given pipe to several inputs.
   * Manages the input and output to delete them when the test case is done.<br>
   * @param tail pipe to run
   * @param inputs a map from input names to the Fields they represent.
   * @param outputs a Fields[] representing output for pipe.getTails() in the same
   *                order.
   * @return a completed flow.
   */
  public Flow runFlow(Pipe[] tails, Map<String, Fields> inputs, Fields[] outputs) {
    String[] pathNames = new String[outputs.length];
    for (int i = 0; i < outputs.length; i++) {
      String sinkName = tails[i].getName();
      pathNames[i] = manageTemporaryPath(sinkName + ".out");
    }
    return runFlow(tails, inputs, outputs, pathNames);
  }

  /**
   * Create and run a cascading flow that connects the given pipe to several inputs.
   * Manages the input and output to delete them when the test case is done.<br>
   * @param tail pipe to run
   * @param inputs a map from input names to the Fields they represent.
   * @param outputs a Fields[] representing output for pipe.getTails() in the same
   *                order.
   * @param outputPaths - The paths to which the output tap is written to.
   * @return a completed flow.
   */
  public Flow runFlow(Pipe[] tails, Map<String, Fields> inputs, Fields[] outputs,
                      String[] outputPaths) {
    Map<String, Tap> sources = new HashMap<String, Tap>();
    for (Map.Entry<String, Fields> input : inputs.entrySet()) {
      Tap tap = new Hfs(new SequenceFile(input.getValue()),
                        manageTemporaryPath(input.getKey()));
      sources.put(input.getKey(), tap);
    }

    if (tails.length != outputs.length) {
      System.err.println("size of tails should be same as outputs.");
      return null;
    }

    int i = 0;
    Map<String, Tap> sinks = new HashMap<String, Tap>();
    for (Fields output : outputs) {
      String sinkName = tails[i].getName();
      Tap sink = new Hfs(new SequenceFile(output), outputPaths[i]);
      sinks.put(sinkName, sink);
      i++;
    }

    Flow f = mFlowConnector.connect(sources, sinks, tails);
    f.complete();
    return f;
  }

  /**
   * Fails Junit if tuple entry iterator entries do not match the expected list.
   *
   * @param results - a tuple entry iterator for tuples output from the flow.
   * @param expected - expected tuple[] to match.
   */
  public void expectResult(TupleEntryIterator results, Tuple[] expected) {
    int resultCount = 0;
    while (results.hasNext()) {
      Tuple actual = results.next().getTuple();
      Assert.assertTrue("Unexpected extra entry: " + actual,
          resultCount < expected.length);
      assertTupleEquals(expected[resultCount], actual);
      resultCount++;
    }
    Assert.assertEquals(expected.length, resultCount);
  }

  /**
   * Fails JUnit if the output of the flow doesn't contain the expected list of tuples.
   * @param flow - a finished cascading flow.
   * @param expected - an array, in order, of the expected output tuples from the flow.
   */
  public void expectResult(Flow flow, Tuple[] expected) throws IOException {
    TupleEntryIterator output = flow.openSink();
    expectResult(output, expected);
  }

  /**
   * Fails JUnit if the output of the given sink for a flow doesn't contain the expected
   * list of tuples.
   * @param flow - a finished cascading flow.
   * @param sinkName - one of the flow's sinks.
   * @param expected - an array, in order, of the expected output tuples from the flow.
   */
  public void expectResult(Flow flow, String sinkName, Tuple[] expected) throws IOException {
    TupleEntryIterator output = flow.openSink(sinkName);
    expectResult(output, expected);
  }

  /**
   * Fails JUnit if the output of the flow doesn't contain all (and only all) the expected tuples.
   * The order of the tuples isn't tested.
   * @param flow - a finished cascading flow.
   * @param expected - an array, in order, of the expected output tuples from the flow.
   */
  public void expectResultUnordered(Flow flow, Tuple[] expected) throws Exception {
    Set<Tuple> expectedSet = new HashSet<Tuple>();
    expectedSet.addAll(Arrays.asList(expected));

    TupleEntryIterator output = flow.openSink();

    while (output.hasNext()) {
      Tuple actual = output.next().getTuple();
      Assert.assertTrue(actual + " doesn't exist in expected.", expectedSet.contains(actual));
      expectedSet.remove(actual);
    }
    Assert.assertEquals("Did not find expected items: " + expectedSet, 0, expectedSet.size());
  }

  /**
   * Returns a List containing all the tuple entries output from this flow.
   * Assumes flow has already run.
   * @return list of TupleEntries output from this flow.
   */
  public List<TupleEntry> getOutput(Flow flow) throws Exception {
    TupleEntryIterator output = flow.openSink();

    List<TupleEntry> result = new ArrayList<TupleEntry>();
    while (output.hasNext()) {
      TupleEntry e = output.next();
      // We have to make a copy of the TupleEntry here -- otherwise cascading
      // will overwrite its values somewhere deep in its innards.
      result.add(new TupleEntry(e));
    }
    return result;
  }

  /**
   * Manages the file with the given name as a temporary path.
   * @param name of file to manage as a temporary input or output.
   * @return the path of this file on HFS.
   */
  public String manageTemporaryPath(String name) {
    String path = getTemporaryFileName(name);
    mPathsToManage.add(path);
    return path;
  }

  private String getTemporaryFileName(String name) {
    return new File(System.getProperty("java.io.tmpdir"), name).getAbsolutePath();
  }

  private void deleteDirectory(String path) throws IOException {
    deleteDirectory(new File(path));
  }

  private void deleteDirectory(File directory) throws IOException {
    if (directory.exists() && directory.isDirectory()) {
      for (File f : directory.listFiles()) {
        if (f.isDirectory()) {
          deleteDirectory(f);
        } else {
          f.delete();
        }
      }
      directory.delete();
    }
  }

  /**
   * Assert that expected is equal to actual, allowing for the fields
   * within expected and actual to have Double values.
   * In those cases, we need to check for equality with an epsilon value.
   */
  public static void assertTupleEquals(Tuple expected, Tuple actual) {
    Assert.assertEquals("Tuple have different num of fields expected:" + expected + " actual:"
        + actual, expected.size(), actual.size());
    boolean mustCompareEachField = false;
    for (int i = 0; i < expected.size(); i++) {
      if (expected.getObject(i) instanceof Double || actual.getObject(i) instanceof Double) {
        mustCompareEachField = true;
      }
    }
    if (mustCompareEachField) {
      for (int i = 0; i < expected.size(); i++) {
        Object e = expected.getObject(i);
        Object a = actual.getObject(i);
        if (e instanceof Double || a instanceof Double) {
          Assert.assertEquals("At position " + i + ": expected tuple: " + expected
              + ", actual tuple: " + actual, (Double)e, (Double)a, 1e-6);
        } else {
          Assert.assertEquals("At position " + i + ": expected tuple: " + expected
              + ", actual tuple: " + actual, e, a);
        }
      }
    } else {
      Assert.assertEquals(expected, actual);
    }
  }
}
