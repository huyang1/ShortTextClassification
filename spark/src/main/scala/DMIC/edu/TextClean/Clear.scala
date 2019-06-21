package DMIC.edu.TextClean

import DMIC.edu.Data.TextBlock
import com.github.javacliparser.StringOption
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

class Clear {

  protected var sep : StringOption =new StringOption("sep",'p',"单词分隔符"," ")

  def clear(data : RDD[String]) : RDD[String] = {
    data.map(_.split(sep.getValue).mkString(" "))
  }

  def clear(data : DStream[String]) : DStream[String] = {
    data.map(_.split(sep.getValue).mkString(" "))
  }

}
