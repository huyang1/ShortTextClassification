package examples.liblinear

import scala.math.{max, min}
import scala.util.control.Breaks._
import breeze.linalg.DenseVector
import examples.FTRLINSHORTTEXT.{FTRL, FTRLUtil}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.{GeneralizedLinearModel, LabeledPoint}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, SVMModel}
import org.apache.spark.rdd.RDD

/**
 * The interface for training liblinear on Spark.
 */
object SparkLiblinear
{
	private def train_one(prob : Problem, param : Parameter, posLabel : Double, localw : Vector
											 ,Z : Vector, N : Vector) : (GeneralizedLinearModel,Vector,Vector) =
	{
		var w : DenseVector[Double] = null
		var z : Vector = null
		var n : Vector = null
		var loss : Double = .0
		/* Construct binary labels.*/
		val binaryProb = prob.genBinaryProb(posLabel)

		val pos = binaryProb.dataPoints.map(point => point.y).filter(_ > 0).count()
		val neg = binaryProb.l - pos
		val primalSolverTol = param.eps * max(min(pos,neg), 1)/binaryProb.l;

		param.solverType match {
			case SolverType.L2_LR => {

				var solver = new Tron(new TronLR())
				w = solver.tron(binaryProb, param, primalSolverTol,localw)
			}
			case SolverType.L2_L2LOSS_SVC => {
				var solver = new Tron(new TronL2SVM())
				w = solver.tron(binaryProb, param, primalSolverTol,localw)
			} case SolverType.FTRL => {
				var ftrl = new FTRLUtil(0,0.01,0.1,0.01)
				ftrl.initPSModel(Z,N)
				val (a,b,c) = ftrl.train(binaryProb)
				z  = a
				n = b
				loss = c
				w = DenseVector(ftrl.weight.toArray)
			}
			case _ => {
				System.err.println("ERROR: unknown solver_type")
				return null
			}
		}
		binaryProb.dataPoints.unpersist()
		var intercept = 0.0
		var weights : Array[Double] = null
		if (prob.bias < 0)
		{
			weights = w.toArray
		}
		else
		{
			weights = w.toArray.slice(0, w.length - 1)
			intercept = w(w.length - 1)
		}

		param.solverType match {
			case SolverType.L2_LR => {
				var model = new LogisticRegressionModel(Vectors.dense(weights), intercept)
				model.clearThreshold()
				(model,z,n)
			}
			case SolverType.L2_L2LOSS_SVC => {
				var model = new SVMModel(Vectors.dense(weights), intercept)
				model.clearThreshold()
				(model,z,n)
			}
			case SolverType.FTRL =>{
				var model = new LogisticRegressionModel(Vectors.dense(weights), intercept)
				model.clearThreshold()
				(model,z,n)
			}
		}
	}

	private def train(prob : Problem, param : Parameter,submodels : LiblinearModel) : LiblinearModel =
	{
		val labels = prob.dataPoints.mapPartitions(blocks =>
		{
			var partition_labels : Set[Double] = Set()
			while (blocks.hasNext)
			{
				partition_labels += blocks.next().y
			}
			Seq(partition_labels).iterator
		}).reduce(_|_)

		val labelSet : Array[Double] = labels.toArray
		var model : LiblinearModel= new LiblinearModel(param, labelSet).setBias(prob.bias)
		if(submodels!=null&&submodels.subModels.length>0) {
			model.setModels(submodels.subModels)
			model.setN(submodels.N)
			model.setZ(submodels.Z)
		}

		if(model.subModels ==null) {
			val (localModel,delaZ,delaN) = train_one(prob, param, model.label(0),Vectors.zeros(50),Vectors.zeros(50),Vectors.zeros(50))
			model.subModels = Array(localModel)
			model.Z = Array(delaZ)
			model.N = Array(delaN)
			model.weights = Array(localModel.weights)
		}else {
			val (localModel,delaZ,delaN) = train_one(prob, param, model.label(0),model.weights(0),model.Z(0),model.N(0))
			model.subModels.update(0,localModel)
			val Z = model.Z(0).toArray.zip(delaZ.toArray).map(x => x._1+x._2)
			model.Z.update(0,Vectors.dense(Z))
			val N = model.N(0).toArray.zip(delaN.toArray).map(x => x._1+x._2)
			model.N.update(0,Vectors.dense(N))
//			model.N.update(0,delaN)
//			model.Z.update(0,delaZ)
			model.weights.update(0,localModel.weights)
		}

		if(labelSet.size > 2)
		{
			for(i <- 1 until labelSet.size)
			{
				if(model.subModels.size <= i) {
					val (localModel,delaZ,delaN) = train_one(prob, param, model.label(0),Vectors.zeros(50),Vectors.zeros(50),Vectors.zeros(50))
					model.subModels = model.subModels :+ localModel
					model.Z = model.Z :+ delaZ
					model.N = model.N :+ delaN
					model.weights = model.weights :+ localModel.weights
				} else {
					val (localModel,delaZ,delaN) = train_one(prob, param, model.label(i),model.weights(i),model.Z(i),model.N(i))
					model.subModels.update(i,localModel)
					val Z = model.Z(i).toArray.zip(delaZ.toArray).map(x => x._1+x._2)
					model.Z.update(i,Vectors.dense(Z))
					val N = model.N(i).toArray.zip(delaN.toArray).map(x => x._1+x._2)
					model.N.update(i,Vectors.dense(N))
//					model.N.update(i,delaN)
//					model.Z.update(i,delaZ)
					model.weights.update(i,localModel.weights)
				}
			}
		}
		if(param.solverType == SolverType.L2_LR)
		{
			model.threshold = 0.5
		}
		model
	}
	
	/**
	 * Show the detailed usage of train.
	 */
	def printUsage() =
	{
		System.err.println("Usage: model = train(trainingData, 'options')")
		printOptions()
	}

	private def printOptions() =
	{
		System.err.println(
			"options:\n"
			+ "-s type : set type of solver (default 0)\n"
			+ "\t0 -- L2-regularized logistic regression (primal)\n"
			+ "\t2 -- L2-regularized L2-loss support vector classification (primal)\n"
			+ "-c cost : set the parameter C (default 1)\n"
			+ "-e epsilon : set tolerance of termination criterion\n"
			+ "\t-s 0 and 2\n"
			+ "\t\t|f'(w)|_2 <= eps*min(pos,neg)/l*|f'(w0)|_2,\n"
			+ "\t\twhere f is the primal function and pos/neg are # of\n"
			+ "\t\tpositive/negative data (default 0.01)\n"
			+ "-B bias : if bias >= 0, instance x becomes [x; bias]; if < 0, no bias term added (default -1)\n"
			+ "-N #salves : if #slaves > 0, enable the coalesce function to reduce the communication cost; if <= 0, do not use the coalesce function (default -1)\n")
	}

	def train(data : RDD[DataPoint]) : LiblinearModel =
	{
		train(data, "",null)
	}

	def train(data : RDD[DataPoint], models : LiblinearModel) : LiblinearModel =
	{
		train(data, "",models)
	}



	
	/**
	 * Train a model given an input RDD of DataPoint.
	 *
	 * @param data an RDD of DataPoint
	 * @param options Liblinear-like options
	 * @return a model
	 */
	def train(data : RDD[DataPoint], options : String,models : LiblinearModel) : LiblinearModel =
	{
		var param = new Parameter()
		val prob = new Problem()
		var model : LiblinearModel = null
	
		/* Parse options */
		var argv = options.trim.split("[ \t]+")
		breakable {
			var i = 0
			while(i < argv.size)
			{
				if(argv(i).size == 0)
				{
					break
				}
				if(argv(i)(0) != '-' || i+1 >= argv.size)
				{
					System.err.println("ERROR: wrong usage")
					printUsage()
					return model
				}
				i += 1
				argv(i-1)(1) match {
					case 's' => param.solverType = SolverType.parse(argv(i).toInt)
					case 'e' => param.eps = argv(i).toDouble
					case 'c' => param.C = argv(i).toDouble
					case 'B' => prob.bias = argv(i).toDouble
					case 'N' => param.numSlaves = argv(i).toInt
					case _ => {
						System.err.println("ERROR: unknown option")
						printUsage()
						return model
					}
				}
				i += 1
			}
		}
		if(param.numSlaves > data.partitions.size)
		{
			param.numSlaves = -1
		}
		param.solverType = SolverType.FTRL

		prob.setData(data.cache())
		train(prob, param,models)
	}
	def train_mllib(data : RDD[LabeledPoint]) : LiblinearModel =
	{
		train_mllib(data,"")
	}

	def train_mllib(data : RDD[LabeledPoint], options : String) : LiblinearModel =
	{
		train(data.map(point => DataPoint.fromMLlib(point)), options,null)
	}
}

