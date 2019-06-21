package DMIC.edu.Feature

import com.github.javacliparser.{IntOption, StringOption}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.random.XORShiftRandom
import org.slf4j.LoggerFactory

/**
  * @ClassName Word2Vec
  * @Description TODO
  * @Author huyang
  * @Date 2018/12/8 14:43
  **/
class WordToVec() extends Trans {
  private val logger = LoggerFactory.getLogger(classOf[WordToVec])

  var model: Word2VecModel = null

  private var ExternalText : StringOption = new StringOption("External text path ", 'e',"外部语料库路径","")

  var features = new IntOption("features", 'f', "文本向量化特征数", 50, 10, Integer.MAX_VALUE)


  override def trans(data : RDD[String]): RDD[Vector] = {

    trainModel(data.sparkContext)

    data.map(x => Vectors.dense(word2vector(x.split(" "))))

  }

  override def trans(data : DStream[String]): DStream[Vector] = {

    trainModel(data.context.sparkContext)

    data.map(x => Vectors.dense(word2vector(x.split(" "))))

  }

  def trainModel(sc : SparkContext): Unit ={

    try {
      val source = sc.textFile(ExternalText.getValue)
      val text = source.map(x => x.split(" ").toSeq)
      model = new Word2Vec().setVectorSize(features.getValue).fit(text)
    } catch {
      case _ => {
        logger.debug("训练外部语料库出错")
      }
    }

  }

  def word2vector(x: Array[String]): Array[Double] = {
    var count =0
    var result = new Array[Double](features.getValue)
    for (word <- x) yield try {count = count+1
    result = result.zip(model.transform(word).toArray).map(t => t._1 + t._2)
    } catch{case _:Exception => result}
    result.map(x => x/count)//是否需要归一化
  }



}
