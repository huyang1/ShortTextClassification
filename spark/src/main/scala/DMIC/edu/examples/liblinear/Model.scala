package examples.liblinear

import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import scala.Predef
import org.apache.spark.mllib.classification.{LogisticRegressionModel,SVMModel}
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.mllib.regression.GeneralizedLinearModel


/**
 * A linear model stores weights and other information.
 *
 *@param param user-specified parameters
 *@param w the weights of features
 *@param nrClass the number of classes
 *@param bias the value of user-specified bias
 */
class LiblinearModel(val param : Parameter, labelSet : Array[Double]) extends Serializable
{
	var label : Array[Double] = labelSet.sortWith(_ < _)
	val nrClass : Int = label.size
	var subModels : Array[GeneralizedLinearModel] = null
	var bias : Double = -1.0
	var threshold : Double = 0.0
	var weights : Array[Vector] = null
	var Z : Array[Vector] = null
	var N : Array[Vector] = null

	def setBias(b : Double) : this.type =
	{
		this.bias = b
		this
	}

	def setModels(models : Array[GeneralizedLinearModel]): this.type = {
		this.subModels = models
    var weight = new Array[Vector](models.length)
    var i = 0
    for(model <- models) {
      weight(i) = model.weights
			i = i+1
    }
    this.weights = weight

		this
	}

	def setZ(Z : Array[Vector]): this.type = {
		this.Z = Z
		this
	}

	def setN(N : Array[Vector]): this.type = {
			this.N = N
		this
	}

	def predictValues(index : Array[Int], value : Array[Double]) : Array[Double] =
	{
		subModels.map(model => model.predict(Vectors.sparse(model.weights.size, index,value)))
	}

	/**
	 *Predict a label given a DataPoint.
	 *@param point a DataPoint
	 *@return a label
	 */
	def predict(point : DataPoint) : (Double,Double) =
	{
		val decValues = predictValues(point.index, point.value)
		println("label is "+point.y+decValues.mkString("\t"))
		var labelIndex = 0
		if(nrClass == 2)
		{
			if(decValues(0) < threshold)
			{
				labelIndex = 1
			}
		}
		else
		{
			var i = 1
			while(i < nrClass)
			{
				if(decValues(i) > decValues(labelIndex))
				{
					labelIndex = i
				}
				i += 1
			}
		}
		(label(labelIndex),decValues(labelIndex))
	}

	/**
	 *Predict probabilities given a DataPoint.
	 *@param point a DataPoint
	 *@return probabilities which follow the order of label
	 */
	def predictProbability(point : DataPoint) : Array[Double] =
	{
		Predef.require(param.solverType == SolverType.L2_LR, "predictProbability only supports for logistic regression.")
		var probEstimates = predictValues(point.index, point.value)
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
}

object LiblinearModel
{

	/**
	 * load Model from the local file system.
	 *
	 * @param fileName path to the input file
	 */
	def loadModel(fileName : String) : LiblinearModel =
	{
		val fis = new FileInputStream(fileName)
		val ois = new ObjectInputStream(fis)
		val model : LiblinearModel = ois.readObject.asInstanceOf[LiblinearModel]
		ois.close
		model
	}
}
