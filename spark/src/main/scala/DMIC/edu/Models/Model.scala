package DMIC.edu.Models

import DMIC.edu.Data.{DataVector, Parameters, TextBlock}
import org.apache.spark.rdd.RDD

/**
  * @ClassName Model
  * @Description TODO
  * @Author huyang
  * @Date 2018/12/7 20:23
  **/
trait Model {

  def predict(sample : DataVector) : Double

  def train(data : TextBlock ,posLabel : String) : Model

}
