//package examples
//
//import examples.liblinear.{DataPoint, LiblinearModel, SparkLiblinear}
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.mllib.evaluation.MulticlassMetrics
//import org.apache.spark.mllib.feature.Word2VecModel
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.rdd.RDD
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object snip {
//  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("TextStream")
//      .setMaster("spark://master:7077")
//      .setJars(List("E:\\repo\\fast-mRMR\\spark\\target\\DMIC-1.0.jar"))
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
//    var ftrlModel : LiblinearModel = null
//
//
//    var index : Array[(String,Double)]= null
//
//
//
//    data.foreachRDD(rdd => {
//
//      if(!rdd.isEmpty()) {
//        val allWord = rdd.flatMap(_._1.toList).count().toDouble
//        val docCount = rdd.count().toDouble
//        val tf = rdd.flatMap(x => x._1.toList.map((_,1))).reduceByKey(_+_).map(x => (x._1,x._2/allWord))
//        val idf = rdd.flatMap(x => x._1.toList.distinct.map((_,1))).reduceByKey(_+_).map(x => (x._1,Math.log10(docCount/x._2+1)))
//        val tfIdf = tf.join(idf,4).map(x => (x._1,x._2._1*x._2._2))
//
//
//        index = tfIdf.collect()
//
//
//        def word2vector(x: Array[String]): Array[Double] = {
//          if(x==null || x.isEmpty) {
//            null
//          } else {
//            var count =.0
//            var result = new Array[Double](50)
//            for (word <- x) yield try {
//              val filter = index.filter(_._1.equals(word))
//              var tfIdf = .0
//              if(filter==null|| filter.size == 0) {
//                tfIdf = .0
//                count = count + tfIdf
//              } else {
//                tfIdf = filter(0)._2
//                count = count + tfIdf
//              }
//
//              result = result.zip(model.transform(word).toArray).map(t => t._1 + t._2*tfIdf)
//            } catch{case _:Exception => result}
//            result.map(x => x/count)//是否需要归一化
//          }
//        }
//        val svm = rdd.sparkContext.parallelize(rdd.collect().map(x => (word2vector(x._1),x._2)))
//
//
//
//      val train = svm.map( x => LabeledPoint(x._2,Vectors.dense(x._1)))
//
//      val svm1= train.map(x => DataPoint.fromLabeledPoint(x))
//
//        var accuracy1 : Double = 1.0
//
//          if(ftrlModel != null) {
//            val result = svm1.map(x =>{
//              val pro = ftrlModel.predict(x)
//              (pro,x.y)
//            })
//
//            val evaluation1 = new MulticlassMetrics(result)
//            accuracy1 = evaluation1.accuracy
//            val weightedPrecision1 = evaluation1.weightedPrecision
//            val weightedRecall1 = evaluation1.weightedRecall
//            val f11 = evaluation1.weightedFMeasure
//
//            println(s"online-SVM classifier 准确率: $accuracy1")
//            println(s"online-SVM classifier 加权准确率: $weightedPrecision1")
//            println(s"online-SVM classifier 加权召回率: $weightedRecall1")
//            println(s"online-SVM classifier F1值: $f11")
//          }
//
//          ftrlModel = SparkLiblinear.train(svm1,ftrlModel)
//        } else println("empty RDD!")
//
//
//
//
//
//
//    })
////    var index : Array[(String,Double)]= null
////
////    if(!(TFIDF == null || TFIDF.isEmpty())) {
////      index = TFIDF.collect()
////    }
//
//
//
////
////    def word2vector(x: Array[String]): Array[Double] = {
////      var count =0
////      var result = new Array[Double](50)
////      for (word <- x) yield try {count = count+1
////      result = result.zip(model.transform(word).toArray).map(t => t._1 + t._2)
////      } catch{case _:Exception => result}
////      result.map(x => x/count)//是否需要归一化
////    }
//
//
////    val train = data.map(x => (word2vector(x._1),x._2)).map( x => LabeledPoint(x._2,Vectors.dense(x._1)))
////
////    val svm1= train.map(x => DataPoint.fromLabeledPoint(x))
////    var ftrlModel : LiblinearModel = null
////    svm1.foreachRDD(rdd => {
////      if(!rdd.isEmpty()) {
////        if(ftrlModel != null) {
////          val result = rdd.map(x =>{
////            val pro = ftrlModel.predict(x)
////            (pro,x.y)
////          })
////          val evaluation1 = new MulticlassMetrics(result)
////          val accuracy1 = evaluation1.accuracy
////          val weightedPrecision1 = evaluation1.weightedPrecision
////          val weightedRecall1 = evaluation1.weightedRecall
////          val f11 = evaluation1.weightedFMeasure
////
////          println(s"online-SVM classifier 准确率: $accuracy1")
////          println(s"online-SVM classifier 加权准确率: $weightedPrecision1")
////          println(s"online-SVM classifier 加权召回率: $weightedRecall1")
////          println(s"online-SVM classifier F1值: $f11")
////        }
////
////
////        ftrlModel = SparkLiblinear.train(rdd,ftrlModel)
////      } else println("empty RDD!")
////
////
////    })
//
//    ssc.start()
//    ssc.awaitTermination()
//
//
//
//  }
//
//}
//
