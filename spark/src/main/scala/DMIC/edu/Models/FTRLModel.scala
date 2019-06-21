package DMIC.edu.Models

/**
  * 每个label对应一个model
  */

import DMIC.edu.Data.{DataVector, Parameters, TextBlock}
import DMIC.edu.classification.FTRL
import breeze.linalg.DenseVector
import com.github.javacliparser.FloatOption
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.slf4j.LoggerFactory
/**
  * @ClassName LinearModel
  * @Description TODO
  * @Author huyang
  * @Date 2018/12/7 19:38
  **/
class FTRLModel() extends Serializable with Model
{

  private val logger = LoggerFactory.getLogger(classOf[FTRLModel])

  var lambda1 = new FloatOption("lambda1", 'l', "FTRL 参数 lambda1", 0.01, 0, Float.MaxValue)
  var lambda2 = new FloatOption("lambda2", 'm', "FTRL 参数 lambda2", 0.01, 0, Float.MaxValue)
  var alpha = new FloatOption("alpha", 'a', "FTRL 参数 alpha", 0.016, 0, Float.MaxValue)
  var beta = new FloatOption("beta", 'b', "FTRL 参数 beta", 0.01, 0, Float.MaxValue)


  var model : GeneralizedLinearModel = null
  var threshold : Double = 0.5
  var weights : DenseVector[Double] = null
  var Z : Vector = null
  var N : Vector = null
  var center : Vector = null
  var params : Vector = null


  override def train(data: TextBlock,posLabel : String): Model = {


    if(this.model == null) {
      val size = data.n

      var newModel = new FTRLModel()

      val binaryProb = data.genTrainVector(posLabel)

      var ftrl = new FTRL(lambda1.getValue,lambda2.getValue,alpha.getValue,beta.getValue)

      ftrl.initPSModel(Vectors.zeros(size),Vectors.zeros(size))

      val (z,n,loss) = ftrl.train(binaryProb)
      newModel.Z  = z
      newModel.N = n
      newModel.weights = DenseVector(ftrl.weight.toArray)

//      newModel.params = Vectors.dense(Array.fill(size)(1.0))
//
//      newModel.center = data.avg(posLabel)


      newModel.lambda1 = this.lambda1
      newModel.lambda2 = this.lambda2
      newModel.alpha = this.alpha
      newModel.lambda1 = this.lambda1

      val model = new LogisticRegressionModel(ftrl.weight, .0)
      model.clearThreshold()
      newModel.model = model

      newModel

    } else {


      val binaryProb = data.genTrainVector(posLabel)

      var ftrl = new FTRL(lambda1.getValue,lambda2.getValue,alpha.getValue,beta.getValue)
      ftrl.initPSModel(Z,N)

      val (z,n,loss) = ftrl.train(binaryProb)
      this.Z  = z
      this.N = n
      this.weights = DenseVector(ftrl.weight.toArray)


      val model = new LogisticRegressionModel(ftrl.weight, .0)
      model.clearThreshold()
      this.model = model

      this

    }

  }



  /**
    * Predict a label given a DataPoint.
    *
    * @param point a DataPoint
    * @return a label
    */
  override def predict(point : DataVector) : Double =
  {

    model.predict(point.data)
  }

}


