package se.uu.farmbio.parsers

import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.DataOutputBuffer

object SDFRecordReader {
  val SIZE_PROPERTY_NAME = "se.uu.farmbio.vs.io.SDFRecordReader.size"
  val DEFAULT_SIZE = "30"
}

class SDFRecordReader(private val split: FileSplit, private val job: JobConf) 
	extends RecordReader[LongWritable, Text] {

  private val path = split.getPath
  private val fs = path.getFileSystem(job)
  private val fsin = fs.open(path)
  private val start = split.getStart
  private val end = start + split.getLength
  private val buffer: DataOutputBuffer = new DataOutputBuffer
  private val END_TAG: Array[Byte] = "$$$$\n".getBytes
  private val size = job.get(SDFRecordReader.SIZE_PROPERTY_NAME,SDFRecordReader.DEFAULT_SIZE).toInt
  private var stillInChunk: Boolean = true
  private var keyCount = 0
  
  fsin.seek(start) 
  if(start != 0) {
	  readUntilMatch(false)
  }
	  
  private def readUntilMatch(withinBlock: Boolean): Boolean = {
    var i = 0
    while (true) {
      val b = fsin.read()
      if (b == -1) {
        return false
      }
      if (withinBlock) {
        buffer.write(b)
      }
      if (b == END_TAG(i)) {
        i += 1
        if (i >= END_TAG.length) {
          return fsin.getPos < end
        }
      } else {
        i = 0
      }
    }
    false
  }

  def close() = {
  }

  def createKey() = {
    var key = new LongWritable(keyCount)
    keyCount += 1
    key
  }

  def createValue() = {
    new Text("")
  }

  def getPos() = {
    fsin.getPos()
  }

  def getProgress() = {
    0
  }
  
  def next(key: LongWritable, value: Text) = {
	  var didRead = false
	  var valueStr = ""
	  1 to size foreach { _ =>
	    val readVal = new Text("")
	    didRead = readOne(new LongWritable, readVal) || didRead
	    valueStr+=readVal.toString
	  }
	  value.set(valueStr.trim)
	  didRead && value.toString!=""
  }

  private def readOne(key: LongWritable, value: Text) = {
    if (stillInChunk) {
      val status = readUntilMatch(true)
      value.set(buffer.getData, 0, buffer.getLength)
      buffer.reset()
      if (!status) {
        stillInChunk = false
      }
      true
    } else {
      false
    }
  }

}