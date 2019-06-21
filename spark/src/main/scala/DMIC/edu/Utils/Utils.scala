package DMIC.edu.Utils

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import DMIC.edu.Models.Model


/**
  * @ClassName Utils
  * @Description TODO
  * @Author huyang
  * @Date 2018/12/7 20:27
  **/

object Utils {

  def loadModel(fileName : String , requiredType: String) : Model =
  {
    val fis = new FileInputStream(fileName)
    val ois = new ObjectInputStream(fis)
    Class.forName(requiredType)
    val model : Model = ois.readObject.asInstanceOf[Model]
    ois.close
    model
  }

  def writeParam(content : String,fileName : String): Unit = {
    val fos = new FileOutputStream(fileName)
    val oos = new ObjectOutputStream(fos)
    oos.write((content+"\r\t").getBytes)
    oos.close
  }


}

object StorageType extends Enumeration{
  type storageType = Value
  val local = Value(0)
  val hdfs = Value(1)
}
