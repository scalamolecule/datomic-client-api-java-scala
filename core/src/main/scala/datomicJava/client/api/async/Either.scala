package datomicJava.client.api.async

abstract class Either[A, B] {
  def isRight: Boolean
  def isLeft: Boolean
}
case class Left[A, B](left_value: A) extends Either[A, B] {
  def isRight: Boolean = false
  def isLeft: Boolean = true
}
case class Right[A, B](right_value: B) extends Either[A, B] {
  def isRight: Boolean = true
  def isLeft: Boolean = false
}