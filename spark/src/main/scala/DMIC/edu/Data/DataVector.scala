package DMIC.edu.Data

import breeze.linalg.max
import org.apache.spark.SparkException
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * @ClassName DataVector
  * @Description TODO
  * @Author huyang
  * @Date 2018/12/7 18:18
  **/

class DataVector (val data : Vector, val y : String) extends Serializable {

  def genTrainVector(posLabel: String): DataVector = {
    var data: Vector = null
    val y = if (this.y == posLabel) "1.0" else "0.0"
    data = this.data
    new DataVector(data,y)
  }

  def getMaxIndex(): Int = {
    if (this.data==null || this.data.size==0) {
      return 0
    }
    this.data.size-1
  }


}
  object DataVector
  {
    def fromLabeledPoint(input : LabeledPoint) : DataVector =
    {
      input.features match {
        case dx: DenseVector => {
          val n = dx.size
          new DataVector(dx, input.label.toString)
        }
        case sx: SparseVector => {
          new DataVector(sx, input.label.toString)
        }
        case _ => throw new SparkException(s"fromMLlib doesn't support ${input.features.getClass}.")
      }
    }

    def fromRDD(rdd : (Vector,String)): DataVector = {
      new DataVector(rdd._1,rdd._2)
    }

    def fromVector(input : Vector) : DataVector =
    {
      input match {
        case dx: DenseVector => {
          new DataVector(dx, "0.0")
        }
        case sx: SparseVector => {
          new DataVector(sx, "0.0")
        }
        case _ => throw new SparkException(s"fromMLlib doesn't support ${input.getClass}.")
      }
    }



  }


