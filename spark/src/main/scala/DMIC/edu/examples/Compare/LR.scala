package examples.Compare

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
/**
  *
  */
object LR {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("LR")
      .setMaster("spark://master:7077")
      .setJars(List("E:\\repo\\fast-mRMR\\spark\\target\\spark-fast-mrmr-0.1.jar"))
    val sc = new SparkContext(conf)

    //val doc = sc.textFile("hdfs://master:9000/root/train/data.csv").repartition(40)
    val doc = sc.textFile("hdfs://master:9000/root/train/5.txt").repartition(40)

    val model = Word2VecModel.load(sc,"hdfs://master:9000/root/model")
    //val model = Word2VecModel.load(sc,"hdfs://master:9000/root/modelNews")

    def word2vector(x: Array[String]): Array[Double] = {
      var count =0
      var result = new Array[Double](50)
      for (word <- x) yield try {count = count+1
      result = result.zip(model.transform(word).toArray).map(t => t._1 + t._2)
      } catch{case _:Exception => result}
      result.map(x => x/count)
    }

    val data = doc.map(x => (x.split("\t")(2).split(" "),x.split("\t")(3).toDouble.toInt-1))
    //val data = doc.map(x => (x.split("\t")(0).split(" "),x.split("\t")(1).toDouble.toInt-1))



    val dataSet = data.map(x => (word2vector(x._1),x._2)).map( x => LabeledPoint(x._2.toInt,Vectors.dense(x._1))).randomSplit(Array(0.7,0.3),1L)

    val train = dataSet(0).map(x => x match {case LabeledPoint(0,_) => x                          case _ => LabeledPoint(1,x.features)})
    //val classifier = new LogisticRegressionWithLBFGS().setNumClasses(4)
    val classifier = new LogisticRegressionWithSGD()

    classifier.optimizer.setStepSize(0.1).setNumIterations(10)


    val LR = classifier.run(train).clearThreshold()

    val test = dataSet(1).map(_.features)
    val label = dataSet(1).map(_.label)

    val result = LR.predict(test)

    println(result.take(5).mkString(","))

//    val evaluation = new MulticlassMetrics(result.zip(label))
//
//    val accuracy = evaluation.accuracy
//    val weightedPrecision = evaluation.weightedPrecision
//    val weightedRecall = evaluation.weightedRecall
//    val f1 = evaluation.weightedFMeasure
//
//    println(s"LR classifier 准确率: $accuracy")
//    println(s"LR classifier 加权准确率: $weightedPrecision")
//    println(s"LR classifier 加权召回率: $weightedRecall")
//    println(s"LR classifier F1值: $f1")

  }

}
