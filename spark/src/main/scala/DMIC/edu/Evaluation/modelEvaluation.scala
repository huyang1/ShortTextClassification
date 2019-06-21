package DMIC.edu.Evaluation

import DMIC.edu.Utils.Utils
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD

/**
  * @ClassName modelEvaluation
  * @Description TODO
  * @Author huyang
  * @Date 2018/12/10 13:45
  **/
object modelEvaluation extends evaluation {

  override def evaluation(rdd : RDD[(Double,Double)]): Double = {

    val evaluation1 = new MulticlassMetrics(rdd)
    val accuracy1 = evaluation1.accuracy
    val weightedPrecision1 = evaluation1.weightedPrecision
    val weightedRecall1 = evaluation1.weightedRecall
    val f11 = evaluation1.weightedFMeasure

    rdd.values.countByValue().map({case (label,count) => print(s"prediction is $label, counter is $count      ")})
    println()
//    rdd.keys.countByValue().map({case (label,count) => print(s"label is $label, counter is $count        ")})
//    println()

    //Utils.writeParam(s"$accuracy1\t$weightedPrecision1\t$weightedRecall1\t$f11","hdfs://master:9000/root/result")

    print(s"online-SVM classifier 准确率: $accuracy1\t")
//    print(s"online-SVM classifier 加权准确率: $weightedPrecision1\t")
//    print(s"online-SVM classifier 加权召回率: $weightedRecall1\t")
//    println(s"online-SVM classifier F1值: $f11")

    accuracy1

  }

}
