package datomicJavaScala.util

object TempId {

  private var tempId = -1000000

  def next: String = {
    tempId -= 1
    s"$tempId"
  }
}
