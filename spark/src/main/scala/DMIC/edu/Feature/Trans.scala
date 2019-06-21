package DMIC.edu.Feature

import DMIC.edu.Data.TextBlock
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

trait Trans {

  def trans(data : RDD[String]) : RDD[Vector]

  def trans(data : DStream[String]) : DStream[Vector]

}
