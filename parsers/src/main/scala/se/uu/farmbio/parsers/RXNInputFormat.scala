package se.uu.farmbio.parsers

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit

class RXNInputFormat extends FileInputFormat[LongWritable,Text] {
  
  def createRecordReader(split: InputSplit , context: TaskAttemptContext): RecordReader[LongWritable,Text] ={
    new RXNRecordReader(split.asInstanceOf[FileSplit], context);
  }                                      

}