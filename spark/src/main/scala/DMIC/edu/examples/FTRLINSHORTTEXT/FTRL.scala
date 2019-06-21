package examples.FTRLINSHORTTEXT


import com.github.fommil.netlib.BLAS
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.{AccumulatorParam, SparkConf, SparkContext}
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.{Seconds, StreamingContext}


object FTRL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TextStream")
      .setMaster("spark://master:7077")
      .setJars(List("E:\\repo\\fast-mRMR\\spark\\target\\spark-fast-mrmr-0.1.jar"))

    @transient
    val sc = new SparkContext(conf)
    val model = Word2VecModel.load(sc,"hdfs://master:9000/root/model-news")

    val ssc = new StreamingContext(sc, Seconds(3))

    Logger.getLogger("org").setLevel(Level.ERROR)

    val stream = ssc.textFileStream("hdfs://master:9000/root/train/")
    //val data = stream.map(x => (x.split("\t")(0).split(" "),x.split("\t")(1).toDouble+1))
    ssc.checkpoint("hdfs://master:9000/root/spark/streaming/stateful/")
    //val data = stream.map(x => (x.split("\t")(2).split(" "),x.split("\t")(3).toDouble))
    val data = stream.map(x => (x.split("\t")(2).split(" "),x.split("\t")(3).toDouble))



    def word2vector(x: Array[String]): Array[Double] = {
      var count =0
      var result = new Array[Double](50)
      for (word <- x) yield try {count = count+1
      result = result.zip(model.transform(word).toArray).map(t => t._1 + t._2)
      } catch{case _:Exception => result}
      result.map(x => x/count)
    }



//    val ftrlUtil = new FTRLUtil(1, 1, 1, 1,0)
//    ftrlUtil.initPSModel(sc,50)
//
//
//
//    val train = data.map(x => (word2vector(x._1),x._2)).map( x => (Vectors.dense(x._1),x._2))
//
//    val train1 = train.map(x => x match {case (_,1.0) => x                          case _ => (x._1,.0)})
//
//    var numBatch = 0
//
//
//    train1.foreachRDD{rdd =>
//
//      numBatch += 1
//      ftrlUtil.updateNZ()
//
//      val aveLossRdd = rdd.repartition(12)
//        .mapPartitions { data =>
//          val dataCollects = data.toArray
//
//          if (dataCollects.length != 0) {
//            val dataVector = dataCollects
//            val batchAveLoss = ftrlUtil.optimize(dataVector,calcGradientLoss)
//
//            Iterator(batchAveLoss)
//          } else {
//            Iterator()
//          }
//        }
//
//      ftrlUtil.updateNZ()
//      ftrlUtil.updateWeight()
//
//      if(!aveLossRdd.isEmpty()) {
//        val globalAveLoss = aveLossRdd.collect
//
//        if (globalAveLoss.length != 0) {
//
//          val globalLoss = globalAveLoss.sum / globalAveLoss.length
//
//          println("the current average loss is:" + globalLoss)
//        }
//      }
//
//    }
//    train1.foreachRDD(rdd => {
//      val result = ftrlUtil.predictOn(rdd.map(_._1)).zip(rdd.map(_._2))
//
//      val evaluation = new MulticlassMetrics(result)
//      val accuracy = evaluation.accuracy
//      val weightedPrecision = evaluation.weightedPrecision
//      val weightedRecall = evaluation.weightedRecall
//      val f1 = evaluation.weightedFMeasure
//
//      println(s"online-SVM classifier 准确率: $accuracy")
//      println(s"online-SVM classifier 加权准确率: $weightedPrecision")
//      println(s"online-SVM classifier 加权召回率: $weightedRecall")
//      println(s"online-SVM classifier F1值: $f1")
//
//    })

    ssc.start()
    // await for application stop
    ssc.awaitTermination()

  }

  private def calcGradientLoss(w: org.apache.spark.mllib.linalg.Vector, label: Double, feature: org.apache.spark.mllib.linalg.Vector)
  : (org.apache.spark.mllib.linalg.Vector, Double) = {
    val blas = BLAS.getInstance()
    val margin = -blas.ddot(w.size,w.toArray,1,feature.toArray,1)

    val gradientMultiplier = 1.0 / (1.0 + math.exp(margin)) - label
    blas.dscal(feature.size,gradientMultiplier,feature.toArray,1)
    val grad = feature


    val loss = if (label > 0) {
      math.log1p(math.exp(margin))//log(x+1)
    } else {
      math.log1p(math.exp(margin)) - margin
    }


    (grad, loss)
  }






}
