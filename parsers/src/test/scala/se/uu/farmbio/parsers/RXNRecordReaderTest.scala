package se.uu.farmbio.parsers

import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RXNRecordReaderTest extends FunSuite {

  test("RXNRecordReader should find the correct number of rxn's") {

    val conf = new Configuration(false);
    conf.set("fs.default.name", "file:///");
    val testFile = new File(getClass.getResource("First10DB2005AllFields.rdf").getPath());
    val testFileSplit = new FileSplit(new Path(testFile.getPath()), 0, testFile.length, null);
    val inputFormat = new RXNInputFormat

    val context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    val reader = inputFormat.createRecordReader(testFileSplit, context);

    reader.initialize(testFileSplit, context);

    var numRXNs = 0;

    while (reader.nextKeyValue()) {
      numRXNs += 1;
    }

    assert(numRXNs == 11, "The Custom reader should read the correct number of records");

  }

}