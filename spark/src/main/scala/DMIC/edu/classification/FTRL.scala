package DMIC.edu.classification

import DMIC.edu.Data.TextBlock
import com.github.fommil.netlib.BLAS
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
  * @ClassName FTRL
  * @Description TODO
  * @Author huyang
  * @Date 2018/12/8 10:40
  **/
class FTRL (lambda1: Double, lambda2: Double, alpha: Double, beta: Double) extends Serializable {


  var zPS : Vector = null
  var nPS : Vector = null

  var weights : Option[Vector] =None


  def initPSModel(Z: Vector, N: Vector): Unit = {
    this.zPS = Z
    this.nPS = N
  }


  def train(prob : TextBlock): (Vector,Vector,Double) = {
      val iter = 2
      var dataVectors = prob.dataVectors

      var delaZsum : Vector = null
      var delaNsum : Vector = null

      val sc = dataVectors.sparkContext

      var loss = .0

      var globalZ = sc.broadcast(zPS)
      var globalN = sc.broadcast(nPS)


      val result = dataVectors.map(x => (x.data,x.y)).repartition(4)
        .mapPartitions(data =>{

          val dataCollects = data.toArray.map(x => (x._1,x._2.toDouble))

          if (dataCollects.length != 0) {
            val dataVector = dataCollects

            val batchAveLoss = this.optimize(dataVector,globalZ.value,globalN.value,calcGradientLoss)

            Iterator(batchAveLoss)
          } else Iterator()
        })

      delaZsum = result.map(_._2).reduce((x1,x2) => {
        Vectors.dense(x1.toArray.zip(x2.toArray).map(x => x._1+x._2))
      })

      delaNsum = result.map(_._3).reduce((x1,x2) => {
        Vectors.dense(x1.toArray.zip(x2.toArray).map(x => x._1+x._2))
      })

      zPS = Vectors.dense(zPS.toArray.zip(delaZsum.toArray).map(x => x._1+x._2))

      nPS = Vectors.dense(nPS.toArray.zip(delaNsum.toArray).map(x => x._1+x._2))

      loss = result.map(_._1).sum()/result.count()

      //println(s"iterator 0 the Loss is $loss")


      for (i <- 0 until iter) {

        globalZ.unpersist()
        globalZ = sc.broadcast(zPS)
        globalN.unpersist()
        globalN = sc.broadcast(nPS)

        val result = dataVectors.map(x => (x.data,x.y)).repartition(4)
          .mapPartitions(data =>{
            val dataCollects = data.toArray.map(x => (x._1,x._2.toDouble))

            if (dataCollects.length != 0) {
              val dataVector = dataCollects
              val batchAveLoss = this.optimize(dataVector,globalZ.value,globalN.value,calcGradientLoss)

              Iterator(batchAveLoss)
            } else Iterator()
          })


        val delaZ = result.map(_._2).reduce((x1,x2) => {
          Vectors.dense(x1.toArray.zip(x2.toArray).map(x => x._1+x._2))
        })


        val delaN = result.map(_._3).reduce((x1,x2) => {
          Vectors.dense(x1.toArray.zip(x2.toArray).map(x => x._1+x._2))
        })

        delaZsum = Vectors.dense(delaZsum.toArray.zip(delaZ.toArray).map(x => x._1+x._2))

        delaNsum = Vectors.dense(delaNsum.toArray.zip(delaN.toArray).map(x => x._1+x._2))

        zPS = Vectors.dense(zPS.toArray.zip(delaZ.toArray).map(x => x._1+x._2))

        nPS = Vectors.dense(nPS.toArray.zip(delaN.toArray).map(x => x._1+x._2))
        val currectLoss = result.map(_._1).sum()/result.count()

        //
        // println(s"iterator $i the Loss is $currectLoss")

      }

      (zPS,nPS,loss)

  }




  def optimize(batch: Array[(Vector, Double)], localZ: Vector, localN: Vector,
               costFun: (Vector, Double, Vector) => (Vector, Double)): (Double,Vector,Vector) = {

    val dim = batch.head._1.size

    var deltaZ = Vectors.zeros(dim)
    var deltaN = Vectors.zeros(dim)

    val lossSum = batch.map { case (feature, label) =>
      val (littleZ, littleN, loss) = optimize(feature, label, localZ, localN,costFun)

      deltaZ = Vectors.dense(deltaZ.toArray.zip(littleZ.toArray).map(x => x._1+x._2))
      deltaN = Vectors.dense(deltaN.toArray.zip(littleN.toArray).map(x => x._1+x._2))

      loss
    }.sum

    (lossSum / batch.length,deltaZ,deltaN)
  }


  def optimize(
                feature: Vector,
                label: Double,
                localZ: Vector,
                localN: Vector,
                costFun: (Vector, Double, Vector) => (Vector, Double)
              ): (Vector, Vector, Double) = {

    val featIndices = 0 until feature.size

    val fetaValues = featIndices.map { fId =>
      val zVal = localZ(fId)
      val nVal = localN(fId)

      updateWeight(fId, zVal, nVal, alpha, beta, lambda1, lambda2)
    }
    val localW = Vectors.dense(fetaValues.toArray)


    val (newGradient, loss) = costFun(localW, label, feature)

    var deltaZ = Vectors.zeros(feature.size)
    var deltaN = Vectors.zeros(feature.size)

    featIndices.foreach { fId =>
      val nVal = localN.apply(fId)
      val gOnId = newGradient.apply(fId)
      val dOnId = 1.0 / alpha * (Math.sqrt(nVal + gOnId * gOnId) - Math.sqrt(nVal))

      deltaZ.toArray.update(fId, gOnId - dOnId * localW.apply(fId))
      deltaN.toArray.update(fId, gOnId * gOnId)
    }
    (deltaZ, deltaN, loss)
  }

  def weight: Vector = {
    val dim = zPS.size
    val wPS = Vectors.zeros(dim)
    assert(zPS.size == nPS.size,"Z N size not equal")

    for (index <- 0 until zPS.size) {
      wPS.toArray.update(index,call(index,zPS.apply(index),nPS.apply(index)))
    }
    wPS
  }



  //W(t,i)更新值
  def updateWeight(
                    fId: Int,
                    zOnId: Double,
                    nOnId: Double,
                    alpha: Double,
                    beta: Double,
                    lambda1: Double,
                    lambda2: Double): Double = {
    if (Math.abs(zOnId) <= lambda1) {
      0.0
    } else {
      (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nOnId)) / alpha)) * (zOnId - Math.signum(zOnId).toInt * lambda1)
    }
  }

  def  call(index :Int, zVal: Double, nVal: Double):Double = {
    if (Math.abs(zVal) > lambda1) {
      return (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
        - Math.signum(zVal) * lambda1);
    } else {
      return 0.0;
    }
  }



  protected def predictPoint(dataMatrix: org.apache.spark.mllib.linalg.Vector) = {

    val margin = BLAS.getInstance.ddot(weights.get.size,weights.get.toArray,1,dataMatrix.toArray,1)
    val score = 1.0 / (1.0 + math.exp(-margin))
    if (score > 0.5) 1.0 else 0.0
  }

  def predictOn(data: RDD[Vector]): RDD[Double] = {
    //    if (weight.size!=) {
    //      throw new IllegalArgumentException("Model must be initialized before starting prediction.")
    //    }
    data.map{x => predictPoint(x)}
  }

  private def calcGradientLoss(w: Vector, label: Double, feature: Vector)
  : (org.apache.spark.mllib.linalg.Vector, Double) = {
    val blas = BLAS.getInstance()
    val margin = -blas.ddot(w.size,w.toArray,1,feature.toArray,1)

    val gradientMultiplier = 1.0 / (1.0 + math.exp(margin)) - label
      blas.dscal(feature.size,gradientMultiplier,feature.toArray,1)
    val grad = feature


    val loss = if (label > 0) {
      math.log1p(math.exp(margin))//log(x+1)
    } else {
      math.log1p(math.exp(margin)) - margin
    }


    (grad, loss)
  }

}

