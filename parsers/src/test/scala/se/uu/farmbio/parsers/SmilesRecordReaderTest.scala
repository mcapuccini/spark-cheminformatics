package se.uu.farmbio.parsers

import org.scalatest.FunSuite
import org.apache.hadoop.mapred.JobConf
import org.junit.runner.RunWith
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.Reporter
import java.io.File
import org.apache.hadoop.mapred.FileSplit
import scala.io.Source
import org.scalatest.junit.JUnitRunner
import org.mockito.Mockito._

@RunWith(classOf[JUnitRunner])
class SmilesRecordReaderTest extends FunSuite {
  
  test("SmilesRecordReader should parse a SMILES file correctly, according to size parameter") {
    
    //Make mocks
    val size = 3
    val jobConf = new JobConf()
    jobConf.set(SmilesRecordReader.SIZE_PROPERTY_NAME, size.toString)
    val testFile = new File(getClass.getResource("molecules.smi").getPath())
    val testFileSplit = new FileSplit(new Path(testFile.getPath()), 0, testFile.length, jobConf)
    val inputFormat = new SmilesInputFormat
    
    //Perform test
	  val recordReader = inputFormat.getRecordReader(testFileSplit, jobConf, mock(classOf[Reporter]))
	  var toTest = ""
	  var key = recordReader.createKey
	  var value = recordReader.createValue
	  while(recordReader.next(key, value)) {
		  val values = value.toString.split("\n")
		  values.foreach(toTest += _+"\n")
		  key = recordReader.createKey
		  value = recordReader.createValue
		  assert(values.length == size || 
		      (values.length < size && !recordReader.next(key,value)))
	  }
	  val testSet = Source.fromFile(getClass.getResource("molecules.smi").getPath).mkString
	  assert(toTest === testSet) 
  
  }
  
}