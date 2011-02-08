namespace java com.tellapart.test.thrift

/** A struct to test Thrift serialization with. */
struct TestStruct {
  1: optional i32 intField,
  2: optional i64 longField,
  3: optional string stringField,
  4: optional list<string> listField,
  5: optional map<string, list<i32>> mapOfLists,
}
