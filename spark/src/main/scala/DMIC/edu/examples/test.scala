//package examples
//
//
//import examples.liblinear.{DataPoint, LiblinearModel, SparkLiblinear}
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.mllib.evaluation.MulticlassMetrics
//import org.apache.spark.mllib.feature.Word2VecModel
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.mllib.regression.{GeneralizedLinearModel, LabeledPoint}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{SparkConf, SparkContext}
//
//object test {
//    def main(args: Array[String]) {
//      val conf = new SparkConf().setAppName("TextStream")
//        .setMaster("spark://master:7077")
//        .setJars(List("E:\\repo\\fast-mRMR\\spark\\target\\spark-fast-mrmr-0.1.jar"))
//      val sc = new SparkContext(conf)
//      val model = Word2VecModel.load(sc,"hdfs://master:9000/root/model-news")
//
//      val ssc = new StreamingContext(sc, Seconds(3))
//
//      Logger.getLogger("org").setLevel(Level.ERROR)
//
//      val stream1 = sc.textFile("hdfs://master:9000/root/train/new-1.txt")
//      val stream2 = sc.textFile("hdfs://master:9000/root/train/new-2.txt")
//
//      //val stream = ssc.textFileStream("hdfs://master:9000/root/train/")
//      val data1 = stream1.map(x => (x.split("\t")(0).split(" "),x.split("\t")(1).toDouble-1))
//      val data2 = stream2.map(x => (x.split("\t")(0).split(" "),x.split("\t")(1).toDouble-1))
//      ssc.checkpoint("hdfs://master:9000/root/spark/streaming/stateful/")
//      //val data1 = stream1.map(x => (x.split("\t")(2).split(" "),x.split("\t")(3).toDouble))
//      //val data2 = stream2.map(x => (x.split("\t")(2).split(" "),x.split("\t")(3).toDouble))
//
//
//
//      def word2vector(x: Array[String]): Array[Double] = {
//        var count =0
//        var result = new Array[Double](50)
//        for (word <- x) yield try {count = count+1
//        result = result.zip(model.transform(word).toArray).map(t => t._1 + t._2)
//        } catch{case _:Exception => result}
//        result.map(x => x/count)//是否需要归一化
//      }
//
//
//      val train1 = data1.map(x => (word2vector(x._1),x._2)).map( x => LabeledPoint(x._2,Vectors.dense(x._1)))
//      val train2 = data2.map(x => (word2vector(x._1),x._2)).map( x => LabeledPoint(x._2,Vectors.dense(x._1)))
//
//      val svm1= train1.map(x => DataPoint.fromLabeledPoint(x))
//      val svm2= train2.map(x => DataPoint.fromLabeledPoint(x))
//      val lib = SparkLiblinear.train(svm1)
//
//      val result1 = train2.map(x =>{
//        val pro = lib.predict(DataPoint.fromLabeledPoint(x))
//        (pro,x.label)
//      })
//      val evaluation1 = new MulticlassMetrics(result1)
//      val accuracy1 = evaluation1.accuracy
//      val weightedPrecision1 = evaluation1.weightedPrecision
//      val weightedRecall1 = evaluation1.weightedRecall
//      val f11 = evaluation1.weightedFMeasure
//
//      println(s"online-SVM classifier 准确率: $accuracy1")
//      println(s"online-SVM classifier 加权准确率: $weightedPrecision1")
//      println(s"online-SVM classifier 加权召回率: $weightedRecall1")
//      println(s"online-SVM classifier F1值: $f11")
//      val LRmodel = SparkLiblinear.train(svm2,lib)
//
//      val result = train1.map(x =>{
//                    val pro = LRmodel.predict(DataPoint.fromLabeledPoint(x))
//                    (pro,x.label)
//                  })
//                  val evaluation = new MulticlassMetrics(result)
//                  val accuracy = evaluation.accuracy
//                  val weightedPrecision = evaluation.weightedPrecision
//                  val weightedRecall = evaluation.weightedRecall
//                  val f1 = evaluation.weightedFMeasure
//
//                  println(s"online-SVM classifier 准确率: $accuracy")
//                  println(s"online-SVM classifier 加权准确率: $weightedPrecision")
//                  println(s"online-SVM classifier 加权召回率: $weightedRecall")
//                  println(s"online-SVM classifier F1值: $f1")
//
//
////      var lib : LiblinearModel = null
////      svm.foreachRDD(rdd => {
////        if(!rdd.isEmpty()) {
////          if(lib==null) {
////            lib = SparkLiblinear.train(rdd)
////          }else lib = SparkLiblinear.train(rdd,lib.subModels)
////        }
////      })
////
////      train.foreachRDD( rdd => {
////        if(!rdd.isEmpty()) {
////          val result = rdd.map(x =>{
////            val pro = lib.predict(DataPoint.fromLabeledPoint(x))
////            (pro,x.label)
////          })
////          val evaluation = new MulticlassMetrics(result)
////          val accuracy = evaluation.accuracy
////          val weightedPrecision = evaluation.weightedPrecision
////          val weightedRecall = evaluation.weightedRecall
////          val f1 = evaluation.weightedFMeasure
////
////          println(s"online-SVM classifier 准确率: $accuracy")
////          println(s"online-SVM classifier 加权准确率: $weightedPrecision")
////          println(s"online-SVM classifier 加权召回率: $weightedRecall")
////          println(s"online-SVM classifier F1值: $f1")
////        }
////      })
////
////      ssc.start()
////      ssc.awaitTermination()
//
//
//
//    }
//  }
//
