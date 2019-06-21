package Feature


import DMIC.edu.Feature.Trans
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.streaming.dstream.DStream

class TFIDF() extends Serializable with Trans {
  var hashTD : HashingTF = new HashingTF()
  var idf : IDF=new IDF()

  override def trans(data : RDD[String]): RDD[Vector] = {

      val tf = hashTD.transform(data.map(_.split(" ").toSeq))
      idf.fit(tf).transform(tf)
  }

  override def trans(data: DStream[String]): DStream[Vector] = {


    data.transform(rdd => {
      val tf = hashTD.transform(rdd.map(_.split(" ").toSeq))
      idf.fit(tf).transform(tf)
    })
  }

  def getIndex(term : Any): Int = {
    hashTD.indexOf(term)
  }

}
