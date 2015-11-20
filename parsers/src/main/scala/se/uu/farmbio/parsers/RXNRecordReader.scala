package se.uu.farmbio.parsers

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileSplit

class RXNRecordReader(private val split: FileSplit, private val context: TaskAttemptContext)
  extends RecordReader[LongWritable, Text] {

  private val start: Long = split.getStart;
  private val end: Long = start + split.getLength;

  private val path = split.getPath;
  private val fs = path.getFileSystem(context.getConfiguration);
  private val fsin: FSDataInputStream = fs.open(path);
  private val buffer: DataOutputBuffer = new DataOutputBuffer;
  private val END_TAG: Array[Byte] = "$RFMT".getBytes;

  private var stillInChunk: Boolean = true;

  private var key: LongWritable = new LongWritable;
  private var value: Text = new Text("");

  def close(): Unit = {
    fsin.close
  }
  def getCurrentKey(): LongWritable = {
    key
  }
  def getCurrentValue(): Text = {
    value
  }
  def getProgress(): Float = {
    0.0f
  }
  def initialize(split: InputSplit, taskContext: TaskAttemptContext): Unit = {
    // we do all initialization in the constructor above
    // Read to start
    fsin.seek(start);
    if (start != 0) {
      readUntilMatch();
    }
  }

  def nextKeyValue(): Boolean = {
    if (!stillInChunk) return false;

    val status = readUntilMatch();

    value = new Text();
    value.set(buffer.getData(), 0, buffer.getLength());
    key = new LongWritable(fsin.getPos());
    buffer.reset();

    // status is true as long as we're still within the
    // chunk we got (i.e., fsin.getPos() < end). If we've
    // read beyond the chunk it will be false
    if (!status) {
      stillInChunk = false;
    }

    return true;
  }

  private def readUntilMatch(): Boolean = {
    var i = 0;
    var b = -1;
    while (true) {
      b = fsin.read();
      if (b == -1) { // if eof - stop reading
        return false;
      }
      buffer.write(b);

      if (b == END_TAG(i)) {
        i += 1;
        if (i >= END_TAG.length) {
          return fsin.getPos < end;
        }
      } else {
        i = 0;
      }
    }
    false;
  }

}