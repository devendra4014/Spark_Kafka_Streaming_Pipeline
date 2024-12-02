package org.example.config

class Parameter extends Serializable {
  private var key: String = _
  private var value: String = _

  def getKey(): String = {
    key
  }

  def getvalue(): String = {
    value
  }

  def setKey(key: String): Unit = {
    this.key = key
  }

  def setValue(value: String): Unit = {
    this.value = value
  }

  override def toString: String = {
    "param [key : " + key + " ,value : " + value + " ]"
  }

}
