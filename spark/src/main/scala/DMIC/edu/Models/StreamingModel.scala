package DMIC.edu.Models

import java.io.{FileOutputStream, ObjectOutputStream}

import DMIC.edu.Data.{DataVector, Parameters, TextBlock}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector

import scala.collection.mutable.ArrayBuffer

/**
  * @ClassName StreamingModel
  * @Description TODO
  * @Author huyang
  * @Date 2018/12/8 16:21
  **/
class StreamingModel(paramer : Parameters) extends Serializable {

  var subModels : Array[Model] = null

  var label : Array[String] = null

  var nrClass : Int = 0

  var historyTopic : ArrayBuffer[Double] = null



   def train(data : TextBlock): Unit = {

    val labels = data.dataVectors.mapPartitions(blocks =>
    {
      var partition_labels : Set[String] = Set()
      while (blocks.hasNext)
      {
        partition_labels += blocks.next().y
      }
      Seq(partition_labels).iterator
    }).reduce(_|_)

    if(label==null || label.isEmpty) {
      label= labels.toArray
      nrClass = label.size
    } else {

      for (a <- labels) {
        if(!label.contains(a)) {
          label = label :+ a
        }
      }
      nrClass = label.size
      }


    if(subModels ==null) {
      val currentModel = paramer.model.train(data,label(0))
      this.subModels = Array(currentModel)
    }else {
      subModels(0).train(data,label(0))

    }

    if(label.size > 1)
    {
      for(i <- 1 until label.size)
      {
        if(subModels.size <= i) {
          val currentModel = paramer.model.train(data,label(i))
          subModels = subModels :+ currentModel
        } else {
          subModels(i).train(data,label(i))
        }
      }
    }


  }

  private def predictValues(data : DataVector) : Array[Double] =
  {
    subModels.map(model => model.predict(data))
  }

  /**
    *Predict a label given a DataPoint.
    *@param point a DataPoint
    *@return a label
    */
  def predict(point : DataVector) : (String,Double) =
  {
    val decValues = predictValues(point)
    var labelIndex = 0

      var i = 1
      while(i < nrClass)
      {
        if(decValues(i) > decValues(labelIndex))
        {
          labelIndex = i
        }
        i += 1
      }

    assert(label.size == decValues.size,"两者不一致")
    assert(nrClass==decValues.size,"error")
    assert(labelIndex < nrClass,"出错")
    (label.toList.apply(labelIndex),decValues(labelIndex))
  }

  /**
    *Predict probabilities given a DataPoint.
    *@param point a DataPoint
    *@return probabilities which follow the order of label
    */
  def predictProbability(point : DataVector) : Array[Double] =
  {
    var probEstimates = predictValues(point)
    // Already prob value in MLlib
    if(nrClass == 2)
    {
      probEstimates = probEstimates :+ 1.0 - probEstimates(0)
    }
    else
    {
      var sum = probEstimates.sum
      probEstimates = probEstimates.map(value => value/sum)
    }
    probEstimates
  }

  /**
    * Save Model to the local file system.
    *
    * @param fileName path to the output file
    */
  def saveModel(fileName : String) =
  {
    val fos = new FileOutputStream(fileName)
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(this)
    oos.close
  }

  def isEmpty() = {
    if(subModels == null || subModels.size ==0) true else false
  }

}