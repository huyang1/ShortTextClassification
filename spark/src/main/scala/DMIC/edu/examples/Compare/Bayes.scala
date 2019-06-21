package examples.Compare

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object Bayes {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("Bayes")
      .setMaster("spark://master:7077")
      .setJars(List("E:\\repo\\fast-mRMR\\spark\\target\\spark-fast-mrmr-0.1.jar"))
    val sc = new SparkContext(conf)


    //val doc = sc.textFile("hdfs://master:9000/root/train/data.csv").repartition(40)
    val doc = sc.textFile("hdfs://master:9000/root/train/new.txt").repartition(40)

//    val wordSet = doc.flatMap(x => x.split("\t")(2).split(" ").toList).distinct().collect()
//    val fileWordList = doc.map(x => x.split("\t")(2).split(" ").toSet)
//    val classList = doc.map(x => x.split("\t")(3).toDouble).collect()
    val wordSet = doc.flatMap(x => x.split("\t")(0).split(" ").toList).distinct().collect()
    val fileWordList = doc.map(x => x.split("\t")(0).split(" ").toSet)
    val classList = doc.map(x => x.split("\t")(1).toDouble).collect()

    def setOfWord2Vec(allWordSet: Array[String] , fileWordList:Array[String])={
      val wSetSize = allWordSet.size
      var vocabArray = Array.fill(wSetSize)(0)
      fileWordList.foreach(x=>{
        if(allWordSet.contains(x)){
          val index = allWordSet.indexOf(x)
          vocabArray(index) = 1
        }
        else{
          println("the word: %s is not in my vocabulary!",x)
        }
      })
      vocabArray.map(x=>x.toDouble)
    }

//    val labelPoints = doc.map({
//      x =>
//        val doc = x.split("\t")(2).split(" ")
//        LabeledPoint(x.split("\t")(3).toDouble, Vectors.dense(setOfWord2Vec(wordSet,doc)))
//    })
val labelPoints = doc.map({
  x =>
    val doc = x.split("\t")(0).split(" ")
    LabeledPoint(x.split("\t")(1).toDouble, Vectors.dense(setOfWord2Vec(wordSet,doc)))
})

      //训练模型
    val data = labelPoints.randomSplit(Array(0.7,0.3),1L)
      val model = NaiveBayes.train(data(0))


      //测试模型
      val predictionAndLabel = data(1).map(p => (model.predict(p.features), p.label))
      //val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / labelPoints.count()
    val evaluation = new MulticlassMetrics(predictionAndLabel)

    val accuracy = evaluation.accuracy
    val weightedPrecision = evaluation.weightedPrecision
    val weightedRecall = evaluation.weightedRecall
    val f1 = evaluation.weightedFMeasure

    println(s"NaiveBayes classifier 准确率: $accuracy")
    println(s"NaiveBayes classifier 加权准确率: $weightedPrecision")
    println(s"NaiveBayes classifier 加权召回率: $weightedRecall")
    println(s"NaiveBayes classifier F1值: $f1")

  }





}
