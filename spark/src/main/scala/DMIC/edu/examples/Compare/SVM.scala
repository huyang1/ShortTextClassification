package examples.Compare

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object SVM {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("SVM")
      .setMaster("spark://master:7077")
      .setJars(List("E:\\repo\\fast-mRMR\\spark\\target\\spark-fast-mrmr-0.1.jar"))
    val sc = new SparkContext(conf)

//    val doc = sc.textFile("hdfs://master:9000/root/train/data.csv").repartition(40)
    val doc = sc.textFile("hdfs://master:9000/root/train/new.txt")

//    val model = Word2VecModel.load(sc,"hdfs://master:9000/root/model")
     val model = Word2VecModel.load(sc,"hdfs://master:9000/root/modelNews")

//    val data = doc.map(x => (x.split("\t")(2).split(" "),x.split("\t")(3).toDouble))
      val data = doc.map(x => (x.split("\t")(0).split(" "),x.split("\t")(1).toDouble))
//
//      val docNum = doc.count()
//
////      val wordNum = doc.flatMap(x => x.split("\t")(2).split(" ").toList).count()
//      val wordNum = doc.flatMap(x => x.split("\t")(0).split(" ").toList).count()
//
//      //val wordPro = doc.flatMap(x => x.split("\t")(2).split(" ").toList).countByValue().map(x => (x._1,x._2*1.0/wordNum))
//      val wordPro = doc.flatMap(x => x.split("\t")(0).split(" ").toList).countByValue().map(x => (x._1,x._2*1.0/wordNum))
//
////      val classPro = doc.map(x => x.split("\t")(3).toDouble.toInt).countByValue().map(x => (x._1,x._2*1.0/docNum))
//      val classPro = doc.map(x => x.split("\t")(1).toDouble.toInt).countByValue().map(x => (x._1,x._2*1.0/docNum))
//
//      val classNum = classPro.size
//
//      /**
//        *获取每个label的word频数
//        */
//        var unionPro = List[Array[(String,Int)]]()
//        for(i <- 1 to classNum){
//          val pro = data.map(x => (x._2,x._1)).filter(_._1 == i).flatMap(_._2.distinct).map((_,1)).reduceByKey(_+_).collect()
//          unionPro = unionPro.::(pro)
//        }
//
//    def computeMutualInfo(x:String,y:Int) : Double ={
//      val Pc = classPro.get(y).get
//      val Pt = wordPro.get(x).get
//      val P = unionPro(y-1).filter(_._1 == x)
//      if(P.size == 0) .0 else {
//        val Ptc = P(0)._2/docNum*1.0
//        val mi = Pc*(math.log(Ptc / (Pt * Pc)) / math.log(2))
//        mi
//      }
//    }

//    def word2vector(x: Array[String],y:Double): Array[Double] = {
//      var count =0
//      var result = new Array[Double](100)
//      var pro = List[Double]()
//
//      val words = x.toList
//      for{
//        word <- words
//      } yield {
//        pro.::(computeMutualInfo(word,y.toInt))
//      }
//
//      val Sum = pro.sum
//
//      for{
//        p <- pro
//      } yield p/Sum
//
//      for {
//        word <- x
//        p <- pro
//      } yield try {count = count+1
//      val vector = model.transform(word).toArray.map(x => x*p)
//
//      result = result.zip(vector).map(t => t._1 + t._2)
//      } catch{case _:Exception => result}
//      result.map(x => x/count)
    //    }
def word2vector(x: Array[String]): Array[Double] = {
  var count =0
  var result = new Array[Double](100)
  for (word <- x) yield try {count = count+1
  result = result.zip(model.transform(word).toArray).map(t => t._1 + t._2)
  } catch{case _:Exception => result}
  result.map(x => x/count)
}


    val train = data.map(x => (word2vector(x._1),x._2)).map( x => LabeledPoint(x._2,Vectors.dense(x._1))).randomSplit(Array(0.7,0.3),8L)

    val train1 = train(0).map(x => x match {case LabeledPoint(1,_) => x case _ => LabeledPoint(0,x.features)})

    val train2 = train(0).filter(_.label != 1).map(x => x match {case LabeledPoint(2,_) => LabeledPoint(1,x.features)
    case _                 => LabeledPoint(0,x.features)})

    val train3 = train(0).filter(_.label != 1).filter(_.label !=2).map(x => x match {case LabeledPoint(3,_) => LabeledPoint(1,x.features)
    case _                 => LabeledPoint(0,x.features)})

    val train4 = train(0).filter(_.label != 1).filter(_.label !=2).filter(_.label !=3).map(x => x match {case LabeledPoint(4,_) => LabeledPoint(1,x.features)
    case _                 => LabeledPoint(0,x.features)})

    val train5 = train(0).filter(_.label != 1).filter(_.label !=2).filter(_.label !=3).filter(_.label !=4).map(x => x match {case LabeledPoint(5,_) => LabeledPoint(1,x.features)
    case _                 => LabeledPoint(0,x.features)})

    val train6 = train(0).filter(_.label != 1).filter(_.label !=2).filter(_.label !=3).filter(_.label !=4).filter(_.label !=5).map(x => x match {case LabeledPoint(6,_) => LabeledPoint(1,x.features)
    case _                 => LabeledPoint(0,x.features)})

    val svm1 = new SVMWithSGD().run(train1,Vectors.zeros(100))
    val svm2= new SVMWithSGD().run(train2,Vectors.zeros(100))
    val svm3 = new SVMWithSGD().run(train3,Vectors.zeros(100))

    val svm4 = new SVMWithSGD().run(train4,Vectors.zeros(100))
    val svm5= new SVMWithSGD().run(train5,Vectors.zeros(100))
    val svm6 = new SVMWithSGD().run(train6,Vectors.zeros(100))

    val test = train(1)


    val result = test.map({
      x =>
        var pre = 1.0
        if(svm1.predict(x.features)== 1.0) pre = 1.0
        else if (svm2.predict(x.features) == 1.0) pre = 2.0
        else if (svm3.predict(x.features) == 1.0) pre = 3.0
        else if (svm4.predict(x.features) == 1.0) pre = 4.0
        else if (svm5.predict(x.features) == 1.0) pre = 5.0
        else if (svm6.predict(x.features) == 1.0) pre = 6.0
        else pre = 7.0
        pre
    })

    val evaluation = new MulticlassMetrics(result.zip(test.map(_.label)))

    val accuracy = evaluation.accuracy
    val weightedPrecision = evaluation.weightedPrecision
    val weightedRecall = evaluation.weightedRecall
    val f1 = evaluation.weightedFMeasure

    println(s"SVM classifier 准确率: $accuracy")
    println(s"SVM classifier 加权准确率: $weightedPrecision")
    println(s"SVM classifier 加权召回率: $weightedRecall")
    println(s"SVM classifier F1值: $f1")





  }

}
