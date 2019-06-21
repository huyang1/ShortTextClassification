//package examples
//
//import examples.liblinear.{DataPoint, LiblinearModel, SparkLiblinear}
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.mllib.evaluation.MulticlassMetrics
//import org.apache.spark.mllib.feature.Word2VecModel
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object StreamFTRL {
//  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("TextStream")
//      .setMaster("spark://master:7077")
//      .setJars(List("E:\\repo\\fast-mRMR\\spark\\target\\spark-fast-mrmr-0.1.jar"))
//    val sc = new SparkContext(conf)
//    val model = Word2VecModel.load(sc,"hdfs://master:9000/root/model-snippet")
//
//    val ssc = new StreamingContext(sc, Seconds(3))
//
//    Logger.getLogger("org").setLevel(Level.ERROR)
//
//    val stream = ssc.textFileStream("hdfs://master:9000/root/train/")
//    ssc.checkpoint("hdfs://master:9000/root/spark/streaming/stateful/")
//    val data = stream.map(x => (x.split("\t")(0).split(" "),x.split("\t")(1).toDouble))
//    //val data = stream.map(x => (x.split("\t")(2).split(" "),x.split("\t")(3).toDouble))
//
//
//        def word2vector(x: Array[String]): Array[Double] = {
//          var count =0
//          var result = new Array[Double](50)
//          for (word <- x) yield try {count = count+1
//          result = result.zip(model.transform(word).toArray).map(t => t._1 + t._2)
//          } catch{case _:Exception => result}
//          result.map(x => x/count)//是否需要归一化
//        }
//
//
//        val train = data.map(x => (word2vector(x._1),x._2)).map( x => LabeledPoint(x._2,Vectors.dense(x._1)))
//
//        val svm1= train.map(x => DataPoint.fromLabeledPoint(x))
//        var ftrlModel : LiblinearModel = null
//        svm1.foreachRDD(rdd => {
//          if(!rdd.isEmpty()) {
//            if(ftrlModel != null) {
//              val result = rdd.map(x =>{
//                val pro = ftrlModel.predict(x)
//                (pro,x.y)
//              })
//              val evaluation1 = new MulticlassMetrics(result)
//              val accuracy1 = evaluation1.accuracy
//              val weightedPrecision1 = evaluation1.weightedPrecision
//              val weightedRecall1 = evaluation1.weightedRecall
//              val f11 = evaluation1.weightedFMeasure
//
//              println(s"online-SVM classifier 准确率: $accuracy1")
//              println(s"online-SVM classifier 加权准确率: $weightedPrecision1")
//              println(s"online-SVM classifier 加权召回率: $weightedRecall1")
//              println(s"online-SVM classifier F1值: $f11")
//            }
//
//
//            ftrlModel = SparkLiblinear.train(rdd,ftrlModel)
//          } else println("empty RDD!")
//
//
//        })
//
//    ssc.start()
//    ssc.awaitTermination()
//
//
//
//  }
//
//}
