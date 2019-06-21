package examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.Vectors

object Word {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test Model")
      .setMaster("spark://master:7077")
      .setJars(List("E:\\repo\\fast-mRMR\\spark\\target\\DMIC-1.0.jar"))
    val sc = new SparkContext(conf)
    var model = Word2VecModel.load(sc,"hdfs://master:9000/root/model-news")
    model.transform("salad")

    val data = "hormones better soy hot flashes"
    val b = data.split(" ")
    var c = b.map(x => model.transform(x))
    var result = c.foldLeft(Array.fill(50)(0.0))((sum,x)=> sum.zip(x.toArray).map(x => x._1+x._2))

    result.map(println(_))

  }

}
