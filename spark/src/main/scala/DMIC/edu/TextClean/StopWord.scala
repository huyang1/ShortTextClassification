package DMIC.edu.TextClean

import java.io.FileNotFoundException

import com.github.javacliparser.StringOption
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory

/**
  * @ClassName StopWord
  * @Description TODO
  * @Author huyang
  * @Date 2018/12/8 11:11
  **/
class StopWord extends Clear {
  private val logger = LoggerFactory.getLogger(classOf[StopWord])

  private var stopWordsPath : StringOption =new StringOption("stopWordsPath",'w',"停用词文件路径","")

  private var storageType : StringOption = new StringOption("storage Type", 's',"文件存储类型","local")

  override def clear(data: RDD[String]): RDD[String] = {
    var stopWords : Array[String] = null
    val sc = data.sparkContext
    try {
      if(storageType.getValue == "local") stopWords = sc.textFile("file://"+stopWordsPath.getValue).map(_.toString).collect()

      else if(storageType.getValue == "hdfs") stopWords = sc.textFile("hdfs://"+stopWordsPath.getValue).map(_.toString).collect()
    } catch {
      case ex: FileNotFoundException =>{
        logger.debug("filePath Not Found", stopWordsPath.getValue)
      }
      case _ => {
        logger.debug("stopWordFile cant trans string")
      }
    }
    if(stopWords == null || stopWords.size==0) {
      logger.debug("stopWordFile is empty!")
      data
    } else {
      data.map(x => x.split(sep.getValue).filter(!stopWords.contains(_)).mkString(" "))
    }
  }

  override def clear(data: DStream[String]): DStream[String] = {
    var stopWords : Array[String] = null
    val sc = data.context.sparkContext
    try {
      if(storageType.getValue == "local") stopWords = sc.textFile("file://"+stopWordsPath.getValue).map(_.toString).collect()

      else if(storageType.getValue == "hdfs") stopWords = sc.textFile("hdfs://"+stopWordsPath.getValue).map(_.toString).collect()
    } catch {
      case ex: FileNotFoundException =>{
        logger.debug("filePath Not Found", stopWordsPath.getValue)
      }
      case _ => {
        logger.debug("stopWordFile cant trans string")
      }
    }
    if(stopWords == null || stopWords.size==0) {
      logger.debug("stopWordFile is empty!")
      data
    } else {
      data.map(x => x.split(sep.getValue).filter(!stopWords.contains(_)).mkString(" "))
    }
  }


}
