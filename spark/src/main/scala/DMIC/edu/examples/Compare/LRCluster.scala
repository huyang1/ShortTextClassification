package examples.Compare

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg.{DenseMatrix, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object LRCluster {



  var weightList = List[(Int,Int,LogisticRegressionModel)]()


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LRCluster")
      .setMaster("spark://master:7077")
      .setJars(List("E:\\repo\\fast-mRMR\\spark\\target\\spark-fast-mrmr-0.1.jar"))

    val sc = new SparkContext(conf)
    val model = Word2VecModel.load(sc, "hdfs://master:9000/root/model-news")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val stream = sc.textFile("hdfs://master:9000/root/train/news-sample.txt")
    val data = stream.map(x => (x.split("\t")(0).split(" "), x.split("\t")(1).toDouble + 1))



    def word2vector(x: Array[String]): Array[Double] = {
      var count = 0
      var result = new Array[Double](50)
      for (word <- x) yield try {
        count = count + 1
        result = result.zip(model.transform(word).toArray).map(t => t._1 + t._2)
      } catch {
        case _: Exception => result
      }
      result.map(x => x / count)
    }


    val train = data.map(x => (word2vector(x._1), x._2)).map(x => LabeledPoint(x._2, Vectors.dense(x._1)))

    val svm = new LogisticRegressionWithSGD()
    svm.optimizer.setStepSize(0.1).setNumIterations(10)

    for (i <- 1 to 7) {
      for (j <- (i+1) to 7) {

        var LRmodel = new LogisticRegressionModel(Vectors.zeros(50), 0.0)

        LRmodel = svm.run(train.filter(x => (x.label==i || x.label==j)).map(x => x match {
          case LabeledPoint(i, _) => LabeledPoint(1, x.features)
          case _ => LabeledPoint(0, x.features)
        }), Vectors.zeros(50))
        weightList = weightList :+ (i,j,LRmodel)

      }
    }

    println(weightList.size)

    var temp = List[Array[Double]]()

    for (block <- weightList) {
      val model = block._3
      val pos = block._1.toDouble
      val neg = block._2.toDouble
      val data = train.map(x => model.predict(x.features)).collect().map{x =>
        if(x > .0)
          pos
        else
          neg
      }
      temp = temp :+ data
    }

    val label = train.map(_.label).collect()



    var m = List[Double]()

    for(index <- 0 until label.length) {
      val data = temp.map(_(index))
      m = m.:+(sc.parallelize(data).countByValue().maxBy(_._2)._1)
    }

    println(label.zip(m).filter(x => x._1 == x._2).length*1.0/label.length)


//    println(temp(0).length)
//
//
//
//    val result = train.map{ x=>
//      var result = Array.fill(7)(0)
//      for (i <- 1 to 7) {
//        for (j <- (i+1) to 7) {
//          println(i)
//          val model = weightList.filter(_._1 == i).filter(_._2 == j)(0)._3
//          if(model.predict(x.features)==1.0) result(i-1) = result(i-1)+1
//          else result(j-1) = result(j-1)+1
//        }
//      }
//      val label = result.zipWithIndex.maxBy(_._1)._2+1.0
//      (label,x.label)
//    }
//
//
//
//
//
////        LRmodel1 = svm.run(train.map(x => x match {
////          case LabeledPoint(1, _) => LabeledPoint(1, x.features)
////          case _ => LabeledPoint(0, x.features)
////        }), LRmodel1.weights).clearThreshold()
////
////
////        LRmodel2 = svm.run(train.map(x => x match {
////          case LabeledPoint(2, _) => LabeledPoint(1, x.features)
////          case _ => LabeledPoint(0, x.features)
////        }), LRmodel2.weights).clearThreshold()
////
////        LRmodel3 = svm.run(train.map(x => x match {
////          case LabeledPoint(3, _) => LabeledPoint(1, x.features)
////          case _ => LabeledPoint(0, x.features)
////        }), LRmodel3.weights).clearThreshold()
////
////        LRmodel4 = svm.run(train.map(x => x match {
////          case LabeledPoint(4, _) => LabeledPoint(1, x.features)
////          case _ => LabeledPoint(0, x.features)
////        }), LRmodel4.weights).clearThreshold()
////
////        LRmodel5 = svm.run(train.map(x => x match {
////          case LabeledPoint(5, _) => LabeledPoint(1, x.features)
////          case _ => LabeledPoint(0, x.features)
////        }), LRmodel4.weights).clearThreshold()
////
////        LRmodel6 = svm.run(train.map(x => x match {
////          case LabeledPoint(6, _) => LabeledPoint(1, x.features)
////          case _ => LabeledPoint(0, x.features)
////        }), LRmodel4.weights).clearThreshold()
////
////        LRmodel7 = svm.run(train.map(x => x match {
////          case LabeledPoint(7, _) => LabeledPoint(1, x.features)
////          case _ => LabeledPoint(0, x.features)
////        }), LRmodel4.weights).clearThreshold()
////
////    //train = split(1)
////
////        val temp = LRmodel1.predict(train.map(_.features))
////          .zip(LRmodel2.predict(train.map(_.features)))
////          .zip(LRmodel3.predict(train.map(_.features))).map(x => List(x._1._1, x._1._2, x._2))
////          .zip(LRmodel4.predict(train.map(_.features))).map(x => x._1 :+ x._2)
////          .zip(LRmodel5.predict(train.map(_.features))).map(x => x._1 :+ (x._2))
////          .zip(LRmodel6.predict(train.map(_.features))).map(x => x._1 :+ (x._2))
////          .zip(LRmodel7.predict(train.map(_.features))).map(x => x._1 :+ (x._2)).map(x => x.zipWithIndex.maxBy(_._1)._2+1.0)
////    val result = temp.zip(train.map(_.label))
//
//
//    var evaluation = new MulticlassMetrics(result)
//    var accuracy = evaluation.accuracy
//    var weightedPrecision = evaluation.weightedPrecision
//    var weightedRecall = evaluation.weightedRecall
//    var f1 = evaluation.weightedFMeasure
//
//    println(s"online-SVM classifier 准确率: $accuracy")
//    println(s"online-SVM classifier 加权准确率: $weightedPrecision")
//    println(s"online-SVM classifier 加权召回率: $weightedRecall")
//    println(s"online-SVM classifier F1值: $f1")



  }

}
