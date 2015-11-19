package se.uu.farmbio.parsers

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.LineRecordReader
import org.apache.hadoop.mapred.RecordReader

object SmilesRecordReader {
  val SIZE_PROPERTY_NAME = "se.uu.farmbio.vs.io.SmilesRecordReader.size"
  val DEFAULT_SIZE = "80"
}

class SmilesRecordReader(private val split: FileSplit, private val job: JobConf)
  extends RecordReader[LongWritable, Text] {

  private val lineReader = new LineRecordReader(job, split)
  private val size = job.get(SmilesRecordReader.SIZE_PROPERTY_NAME,SmilesRecordReader.DEFAULT_SIZE).toInt

  def close() = {
    lineReader.close
  }

  def createKey() = {
    new LongWritable(lineReader.getPos)
  }

  def createValue() = {
    new Text("")
  }

  def getPos() = {
    lineReader.getPos
  }

  def getProgress() = {
    lineReader.getProgress
  }

  def next(key: LongWritable, value: Text) = {
    val smiles = ""
    var didRead = false
	var valueStr = ""
    1 to size foreach { _ =>
      val readVal = new Text("")
      didRead = lineReader.next(new LongWritable, readVal) || didRead
      valueStr+=readVal.toString+"\n"
    }
    value.set(valueStr)
	didRead 
  }

}