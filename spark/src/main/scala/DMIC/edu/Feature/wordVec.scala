package DMIC.edu.Feature


import org.apache.spark.rdd.RDD

import scala.util.Random
import com.github.fommil.netlib.BLAS.{getInstance => blas}

import scala.collection.mutable

/**
  * @ClassName wordVec
  * @Description TODO
  * @Author huyang
  * @Date 2018/12/27 10:26
  **/
class wordVec(val original : Map[String,Array[Float]]) extends Serializable {
  var model = original
  val frist = original
  var bcVocab = mutable.HashMap.empty[String, Int]
  case class VocabWord(
                        var word: String,
                        var cn: Int,
                        var point: Array[Int],
                        var code: Array[Int],
                        var codeLen: Int
                      )
  var vocabSize = 0
  @transient var vocab : Array[VocabWord] = null
  var expTable = createExpTable()




  def init(rdd : RDD[Array[String]]): Unit = {

    val words = rdd.flatMap(x => x)

    vocab = words.map(w => (w, 1))
      .reduceByKey(_ + _)
      .map(x => VocabWord(
        x._1,
        x._2,
        new Array[Int](40),
        new Array[Int](40),
        0))
      .collect()
      .sortWith((a, b) => a.cn > b.cn)

    vocabSize = vocab.size

    var a = 0
    while (a < vocabSize) {
      bcVocab += vocab(a).word -> a

      a += 1
    }


  }


  private def createBinaryTree(): Unit = {
    val count = new Array[Long](vocabSize * 2 + 1)
    val binary = new Array[Int](vocabSize * 2 + 1)
    val parentNode = new Array[Int](vocabSize * 2 + 1)
    val code = new Array[Int](40)
    val point = new Array[Int](40)
    var a = 0
    while (a < vocabSize) {
      count(a) = vocab(a).cn
      a += 1
    }
    while (a < 2 * vocabSize) {
      count(a) = 1e9.toInt
      a += 1
    }
    var pos1 = vocabSize - 1
    var pos2 = vocabSize

    var min1i = 0
    var min2i = 0

    a = 0
    while (a < vocabSize - 1) {
      if (pos1 >= 0) {
        if (count(pos1) < count(pos2)) {
          min1i = pos1
          pos1 -= 1
        } else {
          min1i = pos2
          pos2 += 1
        }
      } else {
        min1i = pos2
        pos2 += 1
      }
      if (pos1 >= 0) {
        if (count(pos1) < count(pos2)) {
          min2i = pos1
          pos1 -= 1
        } else {
          min2i = pos2
          pos2 += 1
        }
      } else {
        min2i = pos2
        pos2 += 1
      }
      count(vocabSize + a) = count(min1i) + count(min2i)
      parentNode(min1i) = vocabSize + a
      parentNode(min2i) = vocabSize + a
      binary(min2i) = 1
      a += 1
    }
    // Now assign binary code to each vocabulary word
    var i = 0
    a = 0
    while (a < vocabSize) {
      var b = a
      i = 0
      while (b != vocabSize * 2 - 2) {
        code(i) = binary(b)
        point(i) = b
        i += 1
        b = parentNode(b)
      }
      vocab(a).codeLen = i
      vocab(a).point(0) = vocabSize - 2
      b = 0
      while (b < i) {
        vocab(a).code(i - b - 1) = code(b)
        vocab(a).point(i - b) = point(b) - vocabSize
        b += 1
      }
      a += 1
    }
  }

  private def createExpTable(): Array[Float] = {
    val expTable = new Array[Float](1000)
    var i = 0
    while (i < 1000) {
      val tmp = math.exp((2.0 * i / 1000 - 1.0) * 6)
      expTable(i) = (tmp / (tmp + 1.0)).toFloat
      i += 1
    }
    expTable
  }



  def update(data : RDD[Array[String]]): Unit = {

    bcVocab.clear()


    val GlobalLabel = data.sparkContext.broadcast(frist.keySet)

    val rdd = data.filter(x => (x.toSet & GlobalLabel.value).size == 0).cache()

    if(rdd.count()<1) {
      return
    }

    init(rdd)

    createBinaryTree()

    val sc = rdd.sparkContext
    val vec = vocab.map(_.word)


    var temp : List[Float]= List[Float]()

    println("init")

    for(i <- vec) {
      val vector = model.get(i)
      if(vector != null && !vector.isEmpty) {
        temp = temp ++ (vector.get.toList)
      }else {
        temp = temp ++ Array.fill[Float](50)((Random.nextFloat() - 0.5f) / 50)
      }
    }

    val syn1Global = new Array[Float](vocab.length * 50)

    val syn0Global = temp.toArray

    require(syn0Global.length==syn1Global.length,"大小不一致")

    val bcSyn0Global = sc.broadcast(syn0Global)
    val bcSyn1Global = sc.broadcast(syn1Global)
    val GlobalVocab = sc.broadcast(vocab)
    val GlobalbcVocab = sc.broadcast(bcVocab)



    val sentences: RDD[Array[Int]] = rdd.mapPartitions { sentenceIter =>
      // Each sentence will map to 0 or more Array[Int]
      sentenceIter.flatMap { sentence =>

        // Sentence of words, some of which map to a word index
        val wordIndexes = sentence.flatMap(GlobalbcVocab.value.get)
        // break wordIndexes into trunks of maxSentenceLength when has more

        Iterable(wordIndexes)

      }
    }
    var learningRate = 0.025
    val numPartitions = 4
    var alpha = learningRate
    val window = 3
    val vectorSize = 50
    val newSentences = sentences.repartition(numPartitions).cache()


    val partial = newSentences.mapPartitions { case iter =>

      val syn0Modify = new Array[Int](vocabSize)
      val syn1Modify = new Array[Int](vocabSize)


      val aaa = iter.foldLeft((bcSyn0Global.value, bcSyn1Global.value)) {
        case ((syn0, syn1), sentence) =>
          var pos = 0
          while (pos < sentence.length) {
            val word = sentence(pos)
            val b = Random.nextInt(window)
            // Train Skip-gram
            var a = b
            while (a < window * 2 + 1 - b) {
              if (a != window) {
                val c = pos - window + a
                if (c >= 0 && c < sentence.length) {
                  val lastWord = sentence(c)
                  val l1 = lastWord * vectorSize
                  val neu1e = new Array[Float](vectorSize)
                  // Hierarchical softmax
                  var d = 0
                  while (d < GlobalVocab.value(word).codeLen) {
                    val inner = GlobalVocab.value(word).point(d)

                    val l2 = inner * vectorSize
                    // Propagate hidden -> output

                    var f1 = blas.sdot(vectorSize, syn0, l1, 1, syn1, l2, 1)

                    var f = blas.sdot(vectorSize, syn0, l1, 1, syn1, l2, 1)

                    if (f > -6 && f < 6) {
                      val ind = ((f + 6) * (1000 / 6 / 2.0)).toInt
                      f = expTable(ind)
                      val g = ((1 - GlobalVocab.value(word).code(d) - f) * alpha).toFloat
                      blas.saxpy(vectorSize, g, syn1, l2, 1, neu1e, 0, 1)
                      blas.saxpy(vectorSize, g, syn0, l1, 1, syn1, l2, 1)
                      syn1Modify(inner) += 1
                    }
                    d += 1
                  }
                  blas.saxpy(vectorSize, 1.0f, neu1e, 0, 1, syn0, l1, 1)
                  syn0Modify(lastWord) += 1
                }
              }
              a += 1
            }
            pos += 1
          }
          (syn0, syn1)
      }

      val syn0Local = aaa._1
      val syn1Local = aaa._2
      // Only output modified vectors.
      Iterator.tabulate(vocabSize) { index =>
        if (syn0Modify(index) > 0) {
          Some((index, syn0Local.slice(index * vectorSize, (index + 1) * vectorSize)))
        } else {
          None
        }
      }.flatten ++ Iterator.tabulate(vocabSize) { index =>
        if (syn1Modify(index) > 0) {
          Some((index + vocabSize, syn1Local.slice(index * vectorSize, (index + 1) * vectorSize)))
        } else {
          None
        }
      }.flatten
    }.reduceByKey{ case (v1, v2) =>
      blas.saxpy(vectorSize, 1.0f, v2, 1, v1, 1)
      v1
    }

    println("train")

    val synAgg = partial.collect()

    var i = 0
    while (i < synAgg.length) {
      val index = synAgg(i)._1
      if (index < vocabSize) {
        Array.copy(synAgg(i)._2, 0, syn0Global, index * vectorSize, vectorSize)
      } else {
        Array.copy(synAgg(i)._2, 0, syn1Global, (index - vocabSize) * vectorSize, vectorSize)
      }
      i += 1
    }
    newSentences.unpersist()

    var index = 0

    println("update")

    for(pair <- bcVocab) {
      if(!model.map(_._1).toArray.contains(pair._1)) {
        model += pair._1 -> syn0Global.slice(vectorSize * index, vectorSize * index + vectorSize)
      } else if(!frist.map(_._1).toArray.contains(pair._1)) {
        model.updated(pair._1,syn0Global.slice(vectorSize * index, vectorSize * index + vectorSize))
      }
      index = index + 1
    }

    GlobalLabel.destroy()
    bcSyn0Global.destroy()
    bcSyn1Global.destroy()
    GlobalVocab.destroy()
    GlobalbcVocab.destroy()

    rdd.unpersist()
  }




}
