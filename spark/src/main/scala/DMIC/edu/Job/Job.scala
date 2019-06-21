package DMIC.edu.Job

import java.io.{File, FileInputStream}
import java.util.Properties

import DMIC.edu.Data.{DataVector, Parameters, TextBlock}
import DMIC.edu.Feature.Trans
import DMIC.edu.Models.{FTRLModel, Model, StreamingModel}
import DMIC.edu.TextClean.{Clear, RexFilter, StopWord}
import Feature.TFIDF
import com.github.javacliparser.{ClassOption, StringOption}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

/**
  * @ClassName Job
  * @Description TODO
  * @Author huyang
  * @Date 2018/12/8 14:28
  **/
class Job() {

  var dataBlock : TextBlock= null

  private var trainPath : StringOption =new StringOption("train Path",'i',"训练文件路径","")

  private var testPath : StringOption =new StringOption("test Path",'t',"测试文件路径","")

  var cleanClassOption = new ClassOption("clean class", 'p', "Classifier to clean text", classOf[Clear], classOf[Clear].getName)

  var TransClassOption = new ClassOption("trans class", 'v', "Classifier to trans text", classOf[Trans], classOf[TFIDF].getName)

  var classifityOption = new ClassOption("classifity class", 'c', "Classifier to classifity text", classOf[Model], classOf[FTRLModel].getName)


  def Driver(): Unit = {


//    val conf = new SparkConf().setAppName(props.getProperty("jobName"))
//      .setMaster(props.getProperty("url"))
//      .setJars(List(jarpath))
    val sc = new SparkContext()

    val stream = sc.textFile("hdfs://master:9000/root/train/")

    val labels = stream.map(x => 1.0)

    val data = cleanClassOption.getValue[Clear].clear(stream)

    val temp = TransClassOption.getValue[Trans].trans(data)

    initTextBlock(temp ,labels)



    val param = new Parameters().setNumSlaves(3)
      .setModel(classifityOption.getValue[Model])
      .setDataPath(trainPath.getValue)
      .setFeature(temp.first().size)

    val driver = new JobDriver(param)

    driver.train(dataBlock.dataVectors)

    //driver.predict(/****/)


  }


  def initTextBlock(data : RDD[Vector],labels : RDD[Double]): Unit = {

    assert(data.count() == labels.count(),"instances size not equals labels size!")

    val labelsPoint = data.zip(labels).map(x => LabeledPoint(x._2,x._1))

    dataBlock.setData(labelsPoint.map(x => DataVector.fromLabeledPoint(x)))

  }



}
