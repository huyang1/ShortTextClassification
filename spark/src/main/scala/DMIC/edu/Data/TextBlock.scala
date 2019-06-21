package DMIC.edu.Data

import com.github.fommil.netlib.BLAS
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
  * @ClassName TextBlock
  * @Description TODO
  * @Author huyang
  * @Date 2018/12/7 19:32
  **/

class TextBlock() extends Serializable
{
  var dataVectors : RDD[DataVector] = null
  var n : Int = 0
  var l : Long = 0

  def setData(dataPoints : RDD[DataVector]) : this.type =
  {
    this.dataVectors = dataPoints
    this.l = dataPoints.count()
    this.n = this.dataVectors.first().data.size
    this
  }

  def genTrainVector(posLabel : String) : TextBlock =
  {
    val binaryProb = new TextBlock()
    binaryProb.l = this.l
    binaryProb.n = this.n

    /* Compute Problem Label */
    val dataVectors = this.dataVectors.mapPartitions(blocks => {
      blocks.map(p => p.genTrainVector(posLabel))
    }).cache()
    binaryProb.dataVectors = dataVectors
    binaryProb
  }

//  def avg(posLabel : Double): Vector = {
//    var result = Array.fill(n)(.0)
//    if(dataVectors==null)  Vectors.dense(result)
//    else {
//      val temp = dataVectors.filter(_.y==posLabel).map(_.data.toArray).collect()
//      for(data <- temp) {
//        result = result.zip(data).map(x => x._1+x._2)
//      }
//      Vectors.dense(result.map(_/temp.size))
//    }
//  }

}
