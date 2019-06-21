package DMIC.edu.TextClean

import com.github.javacliparser.StringOption
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
  * @ClassName RexFilter
  * @Description TODO
  * @Author huyang
  * @Date 2018/12/8 10:53
  **/
class RexFilter() extends Clear {

  private val rex = "[^a-z|A-Z|0-9]"

  override def clear(data: RDD[String]): RDD[String] = data.map(x => x.split(sep.getValue).map( _.replaceAll(rex,"")).mkString(" "))

  override def clear(data: DStream[String]): DStream[String] = data.map(x => x.split(sep.getValue).map( _.replaceAll(rex,"")).mkString(" "))




}
