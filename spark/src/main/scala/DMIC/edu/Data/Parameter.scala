package DMIC.edu.Data

import DMIC.edu.Models.Model

/**
  * @ClassName Parameter
  * @Description TODO
  * @Author huyang
  * @Date 2018/12/7 19:59
  **/
class Parameters() extends Serializable
{
  var dataPath : String = ""

  var numSlaves : Int =  -1

  var model : Model= null

  var feature : Int = 1

  def setNumSlaves(slaves : Int): this.type = {
    this.numSlaves = slaves
    this
  }

  def setDataPath(path : String) : this.type = {
    this.dataPath = path
    this
  }

  def setModel(model : Model ) : this.type = {
    this.model = model
    this
  }

  def setFeature(feature : Int ) : this.type = {
    this.feature = feature
    this
  }


}
