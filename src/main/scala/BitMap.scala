package zhiwin.spark.guide

import scala.collection.mutable.ArrayBuffer

trait BitMap {

  def contains(t: Int): Boolean

  def add(t: Int): Unit
}

class BitMapLive(max: Int) extends BitMap {

  val bits = ArrayBuffer.fill[Byte]((max >> 3) + 1)(0)

  override def add(t: Int): Unit = {
    require(t >= 0 && t <= max)

    val idx = t >> 3
    val pos = t & 0x07
    bits(idx) = (bits(idx) | (1 << pos)).toByte
  }

  override def contains(t: Int): Boolean = {
    require(t >= 0 && t <= max)

    val idx = t >> 3
    val pos = t & 0x07

    (bits(idx) & (1 << pos)) != 0
  }
}

// Ref: https://www.jianshu.com/p/e530baada558