package DMIC.edu.Job

import DMIC.edu.Data.{DataVector, Parameters}
import DMIC.edu.Evaluation.modelEvaluation
import DMIC.edu.Feature.{Trans, WordToVec}
import DMIC.edu.Models.{FTRLModel, Model}
import DMIC.edu.TextClean.Clear
import Feature.TFIDF
import breeze.linalg.max
import examples.liblinear.{DataPoint, LiblinearModel, SparkLiblinear}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ClassName test
  * @Description TODO
  * @Author huyang
  * @Date 2018/12/10 17:50
  **/
object test {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Stream")
      .setMaster("spark://master:7077")
      .setJars(List("E:\\repo\\fast-mRMR\\spark\\target\\DMIC-1.0.jar"))
    val sc = new SparkContext(conf)
    val model = Word2VecModel.load(sc,"hdfs://master:9000/root/model-news")

    val ssc = new StreamingContext(sc, Seconds(2))

    Logger.getLogger("org").setLevel(Level.ERROR)

    val stream = ssc.textFileStream("hdfs://master:9000/root/train/").repartition(12)
    ssc.checkpoint("hdfs://master:9000/root/spark/streaming/stateful/")

    val data = stream.map(x => x.split(" "))
    val labels = sc.textFile("hdfs://master:9000/root/labels").collect()
    //val data = stream.map(x => (x.split("\t")(0).split(" "),x.split("\t")(1)))


    val param = new Parameters().setNumSlaves(4)
      .setModel(new FTRLModel())
      .setFeature(50)

    def word2vector(x: Array[String]): Array[Double] = {
      var count =0
      var result = new Array[Double](50)
      for (word <- x) yield try {count = count+1
      result = result.zip(model.transform(word).toArray).map(t => t._1 + t._2)
      } catch{case _:Exception => result}
      result.map(x => x/count)//是否需要归一化
    }



    val driver = new JobDriver(param)

    var flag = true

    var index = 0

    data.foreachRDD(rdd => {
      if(!rdd.isEmpty()) {

        rdd.cache()

        val size = rdd.count().toInt
        var label = Array.fill(size)("")
        for(i <- 0 until size) {
          label(i) = labels.apply(i+index)
        }
        index = index + size

        val temp = rdd.zipWithIndex().map(x => (x._1,label.apply(x._2.toInt)))





        val train = temp.map(x => (word2vector(x._1),x._2)).map( x => ((Vectors.dense(x._1)),x._2))

        val svm1= train.map(x => DataVector.fromRDD(x))

        var lastAccuracy = .0


        if(!driver.isEmpty()) {
          val result = driver.predict(svm1)
          val sum = result.count()

          val currentAccuracy = svm1.map(_.y).zip(result.map(_._1)).filter(x => x._1.equals(x._2)).count()*1.0/sum

          println(s"classifier 准确率: $currentAccuracy")

          //利用预测占比和预测概率乘积做为主题强度的标志
          val count = result.countByKey()

          val data = result.groupByKey(4).map(x => (x._1,max(x._2))).collect()


          data.map({case (label,pro) => print(s"topic is $label,  pro is "+pro*(count.get(label).get*1.0/sum)+"\t")})
          println()


          if(lastAccuracy - currentAccuracy>0.2) {
            flag = false
            lastAccuracy = currentAccuracy
            println("drit")
          } else {
            flag = true
            lastAccuracy = currentAccuracy
            println("")
          }




        }
        driver.train(svm1)
        if(!flag) driver.train(svm1)

        //println(driver.classifity.label.mkString(" "))
      } else println("empty RDD!")
    })

    ssc.start()
    ssc.awaitTermination()



  }

}
