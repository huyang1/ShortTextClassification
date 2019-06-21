package DMIC.edu.Evaluation

import org.apache.spark.rdd.RDD

/**
  * @ClassName evaluation
  * @Description TODO
  * @Author huyang
  * @Date 2018/12/10 13:43
  **/
trait evaluation {
  /**
    *
    * @param rdd first col is predict value. the second col is true value.
    */
  def evaluation(rdd : RDD[(Double,Double)]) : Double

}
