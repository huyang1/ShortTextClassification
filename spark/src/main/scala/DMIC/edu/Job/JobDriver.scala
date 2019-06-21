package DMIC.edu.Job

import java.io.{FileInputStream, ObjectInputStream}

import DMIC.edu.Data.{DataVector, Parameters, TextBlock}
import DMIC.edu.Models.StreamingModel
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
  * @ClassName DMIC
  * @Description TODO
  * @Author huyang
  * @Date 2018/12/9 13:48
  **/
class JobDriver(param : Parameters) extends Serializable {

  var classifity = new StreamingModel(param)


  def train(data : DStream[DataVector]): Unit = data.foreachRDD(train(_))

  def train(data : RDD[DataVector]): Unit = classifity.train(new TextBlock().setData(data))

  def predict(data : RDD[DataVector]) = data.map(x =>classifity.predict(x))

  def predict(data : DStream[DataVector])= data.map(x => classifity.predict(x))

  def predictProbability(data : RDD[DataVector]) = data.map(x => classifity.predictProbability(x))

  def predictProbability(data : DStream[DataVector]) = data.map(x => classifity.predictProbability(x))

  def saveModel(path : String) = classifity.saveModel(path)

  def loadModel(fileName : String) = {
    val fis = new FileInputStream(fileName)
    val ois = new ObjectInputStream(fis)
    val model : StreamingModel = ois.readObject.asInstanceOf[StreamingModel]
    ois.close
    classifity = model
  }

  def isEmpty() = classifity.isEmpty()


}





