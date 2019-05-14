package lxpsee.top.scala4learn.ppt.classDemo

import scala.beans.BeanProperty

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/22 15:50.
  */
class Students {
  @BeanProperty
  var myName: String = "tom"

  def name = "your name is " + myName

  def name_=(newValue: String): Unit = {
    println(" nonononon")
  }


}
