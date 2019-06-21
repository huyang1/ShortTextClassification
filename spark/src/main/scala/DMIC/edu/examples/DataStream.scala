package examples


import examples.stream.StreamingLRWithSGD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DataStream {

  var LRmodel1 = new LogisticRegressionModel(Vectors.zeros(50), 0.0).clearThreshold()
  var LRmodel2 = new LogisticRegressionModel(Vectors.zeros(50), 0.0).clearThreshold()
  var LRmodel3 = new LogisticRegressionModel(Vectors.zeros(50), 0.0).clearThreshold()
  var LRmodel4 = new LogisticRegressionModel(Vectors.zeros(50), 0.0).clearThreshold()




  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TextStream")
      .setMaster("spark://master:7077")
      .setJars(List("E:\\repo\\fast-mRMR\\spark\\target\\spark-fast-mrmr-0.1.jar"))
    val sc = new SparkContext(conf)
    val model = Word2VecModel.load(sc,"hdfs://master:9000/root/model")

    val ssc = new StreamingContext(sc, Seconds(3))

    Logger.getLogger("org").setLevel(Level.ERROR)

    val stream = ssc.textFileStream("hdfs://master:9000/root/train/").repartition(40)
    ssc.checkpoint("hdfs://master:9000/root/spark/streaming/stateful/")
    val data = stream.map(x => (x.split("\t")(2).split(" "),x.split("\t")(3).toDouble))



    def word2vector(x: Array[String]): Array[Double] = {
      var count =0
      var result = new Array[Double](50)
      for (word <- x) yield try {count = count+1
      result = result.zip(model.transform(word).toArray).map(t => t._1 + t._2)
      } catch{case _:Exception => result}
      result.map(x => x/count)
    }


    val train = data.map(x => (word2vector(x._1),x._2)).map( x => LabeledPoint(x._2,Vectors.dense(x._1)))






//    val svm1 = new StreamingLRWithSGD().setStepSize(0.1).setNumIterations(10).setInitialWeights(Vectors.zeros(50))
//    val svm2 = new StreamingLRWithSGD().setStepSize(0.1).setNumIterations(10).setInitialWeights(Vectors.zeros(50))
//    val svm3 = new StreamingLRWithSGD().setStepSize(0.1).setNumIterations(10).setInitialWeights(Vectors.zeros(50))
//    val svm4 = new StreamingLRWithSGD().setStepSize(0.1).setNumIterations(10).setInitialWeights(Vectors.zeros(50))

    val svm = new LogisticRegressionWithSGD()
    svm.optimizer.setStepSize(0.1).setNumIterations(10)



//
//    val train1 = train.map(x => x match {case LabeledPoint(1,_) => x                          case _ => LabeledPoint(0,x.features)})
//
//    val train2 = train.map(x => x match {case LabeledPoint(2,_) => LabeledPoint(1,x.features) case _ => LabeledPoint(0,x.features)})
//
//    val train3 = train.map(x => x match {case LabeledPoint(3,_) => LabeledPoint(1,x.features) case _ => LabeledPoint(0,x.features)})
//
//    val train4 = train.map(x => x match {case LabeledPoint(4,_) => LabeledPoint(1,x.features) case _ => LabeledPoint(0,x.features)})


//    val train2 = train.filter(_.label != 1).map(x => x match {case LabeledPoint(2,_) => LabeledPoint(1,x.features)
//                                                               case _                 => LabeledPoint(0,x.features)})
//
//    val train3 = train.filter(_.label != 1).filter(_.label !=2).map(x => x match {case LabeledPoint(3,_) => LabeledPoint(1,x.features)
//                                                                                   case _                 => LabeledPoint(0,x.features)})


    train.foreachRDD(rdd =>
      if(!rdd.isEmpty()) {
        LRmodel1 = svm.run(rdd.map(x => x match {
          case LabeledPoint(1,_) => LabeledPoint(1,x.features)
          case _ => LabeledPoint(0,x.features)}),LRmodel1.weights).clearThreshold()


        LRmodel2 = svm.run(rdd.map(x => x match {
          case LabeledPoint(2,_) => LabeledPoint(1,x.features)
          case _ => LabeledPoint(0,x.features)}),LRmodel2.weights).clearThreshold()

        LRmodel3 = svm.run(rdd.map(x => x match {
          case LabeledPoint(3,_) => LabeledPoint(1,x.features)
          case _ => LabeledPoint(0,x.features)}),LRmodel3.weights).clearThreshold()

        LRmodel4 = svm.run(rdd.map(x => x match {
          case LabeledPoint(4,_) => LabeledPoint(1,x.features)
          case _ => LabeledPoint(0,x.features)}),LRmodel4.weights).clearThreshold()

        svm.optimizer.setStepSize(0.01).setNumIterations(10)

        val temp = LRmodel1.predict(rdd.map(_.features)).zip(LRmodel2.predict(rdd.map(_.features)))
          .zip(LRmodel3.predict(rdd.map(_.features))).map(x => List(x._1._1,x._1._2,x._2))
          .zip(LRmodel4.predict(rdd.map(_.features))).map(x => x._1 :+ (x._2)).map(x => x.zipWithIndex.maxBy(_._1)._2+1.0)
        val result = temp.zip(rdd.map(_.label))


        val evaluation = new MulticlassMetrics(result)
        val accuracy = evaluation.accuracy
        val weightedPrecision = evaluation.weightedPrecision
        val weightedRecall = evaluation.weightedRecall
        val f1 = evaluation.weightedFMeasure

        println(s"online-SVM classifier 准确率: $accuracy")
        println(s"online-SVM classifier 加权准确率: $weightedPrecision")
        println(s"online-SVM classifier 加权召回率: $weightedRecall")
        println(s"online-SVM classifier F1值: $f1")

      } else {
        println("empty RDD")
      }

    )

//
//    train1.foreachRDD(rdd =>
//      if (!rdd.isEmpty) {
//        LRmodel1 = svm.run(rdd,LRmodel1.weights).clearThreshold()
//        ResultList1.:::(LRmodel1.predict(rdd.map(_.features)).collect().toList)
//
//      }
//    )
//
//    train2.foreachRDD(rdd =>
//      if (!rdd.isEmpty) {
//        LRmodel2 = svm.run(rdd,LRmodel2.weights).clearThreshold()
//        ResultList2.:::(LRmodel1.predict(rdd.map(_.features)).collect().toList)
//      }
//    )
//
//    train3.foreachRDD(rdd =>
//      if (!rdd.isEmpty) {
//        LRmodel3 = svm.run(rdd,LRmodel3.weights).clearThreshold()
//        ResultList3.:::(LRmodel1.predict(rdd.map(_.features)).collect().toList)
//      }
//    )
//
//    train4.foreachRDD(rdd =>
//      if (!rdd.isEmpty) {
//        LRmodel4 = svm.run(rdd,LRmodel4.weights).clearThreshold()
//        ResultList4.:::(LRmodel1.predict(rdd.map(_.features)).collect().toList)
//      }
//    )
//
//    require(ResultList1.size==ResultList2.size)
//    require(ResultList3.size==ResultList2.size)
//    require(ResultList3.size==ResultList4.size)
//
//
//
//    if(ResultList1.size>0) {
//      println("train"+ResultList1(0))
//    }
//



//    svm1.trainOn(train1)
//    svm2.trainOn(train2)
//    svm3.trainOn(/*train4.map(_._1)union(train3.filter(_.label ==1))*/train3)
//    svm4.trainOn(train4)
//
//    svm1.clearThreshold()
//    svm2.clearThreshold()
//    svm3.clearThreshold()
//    svm4.clearThreshold()




//    val test = train.map(x => (x.label,x.features))

    //val test  = train.filter(_.label == 4.0).map(x => (x,x.features))


//    val result = test.transform{rdd =>
//      rdd.map{x =>
//        val pre = /*List[Double](LRmodel1.predict(x._2),LRmodel2.predict(x._2),LRmodel3.predict(x._2),LRmodel4.predict(x._2))*/
//        (x._1,pre)
//      }
//    }
//    val result = test.map(rdd => {
//        LRmodel1.clearThreshold()
//        LRmodel1.predict(rdd._2)
//    })





//    val prediction1 = svm1.predictOnValues(test)
//    val prediction2 = svm1.predictOnValues(test)
//    val prediction3 = svm1.predictOnValues(test)
//    val prediction4 = svm1.predictOnValues(test)


//    val A = predictions.filter(_._2 == 1.0)
//    val unA = predictions.filter(_._2 ==  .0)
//
//    val predictions1 = svm2.predictOnValues[LabeledPoint](unA.map(x => (x._1, x._1.features)))
//
//    val B = predictions1.filter(_._2 == 1.0).map(x => (x._1,2.0))
//
//    val unB = predictions1.filter(_._2 ==  .0)
//
//    val predictions2 = svm3.predictOnValues[LabeledPoint](unB.map(x => (x._1, x._1.features)))
//
//    val C = predictions2.filter(_._2 == 1.0).map(x => (x._1,3.0))
//
//    val D = predictions2.filter(_._2 == .0).map(x => (x._1,4.0))
//
//
//
//    //val result = A.union(B).union(C).union(D)
//
//
//
//    result.count().print()
//
//
//
//
//
//
//
//    result.foreachRDD({rdd =>
//      println("test"+rdd.take(4).mkString(","))
//      val evaluation = new MulticlassMetrics(rdd.map(x => (x._2.zipWithIndex.maxBy(_._1)._2+1,x._1/*x._2,x._1.label*/)))
//      val accuracy = evaluation.accuracy
//      val weightedPrecision = evaluation.weightedPrecision
//      val weightedRecall = evaluation.weightedRecall
//      val f1 = evaluation.weightedFMeasure
//
//      println(s"online-SVM classifier 准确率: $accuracy")
//      println(s"online-SVM classifier 加权准确率: $weightedPrecision")
//      println(s"online-SVM classifier 加权召回率: $weightedRecall")
//      println(s"online-SVM classifier F1值: $f1")
//    })






    ssc.start()
    ssc.awaitTermination()

  }




}
