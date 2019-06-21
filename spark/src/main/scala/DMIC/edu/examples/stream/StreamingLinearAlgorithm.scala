/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package examples.stream


import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.{GeneralizedLinearAlgorithm, GeneralizedLinearModel, LabeledPoint}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag


abstract class StreamingLinearAlgorithm[
    M <: GeneralizedLinearModel,
    A <: GeneralizedLinearAlgorithm[M]]{

  /** The model to be updated and used for prediction. */
  protected var model: Option[M]

  /** The algorithm to use for updating. */
  protected val algorithm: A


  def latestModel(): M = {
    model.get
  }


  def trainOn(data: DStream[LabeledPoint]): Unit = {
    if (model.isEmpty) {
      throw new IllegalArgumentException("Model must be initialized before starting training.")
    }
    data.foreachRDD { (rdd, time) =>
      if (!rdd.isEmpty) {
        model = Some(algorithm.run(rdd, model.get.weights))
      }
    }
  }

  def predictOn(data: DStream[Vector]): DStream[Double] = {
    if (model.isEmpty) {
      throw new IllegalArgumentException("Model must be initialized before starting prediction.")
    }
    data.map{x => model.get.predict(x)}
  }

  def predictOn(data: Vector): Double = {
    if (model.isEmpty) {
      throw new IllegalArgumentException("Model must be initialized before starting prediction.")
    }
    model.get.predict(data)
  }



  def predictOnValues[K: ClassTag](data: DStream[(K, Vector)]): DStream[(K, Double)] = {
    if (model.isEmpty) {
      throw new IllegalArgumentException("Model must be initialized before starting prediction")
    }
    data.mapValues{x => model.get.predict(x)}
  }

}
