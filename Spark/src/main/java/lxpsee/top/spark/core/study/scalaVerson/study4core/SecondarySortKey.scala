package lxpsee.top.spark.core.study.scalaVerson.study4core

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/27 17:19.
  */
class SecondarySortKey(val first: Int, val second: Int)
  extends Ordered[SecondarySortKey] with Serializable {

  override def compare(that: SecondarySortKey): Int = {
    if (this.first - that.first != 0) {
      this.first - that.first
    } else {
      this.second - that.second
    }
  }
}
