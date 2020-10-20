package datomicJava.client.api.async

abstract class Either[A, B] {}
case class Left[A, B](left_value: A) extends Either[A, B] {}
case class Right[A, B](right_value: B) extends Either[A, B] {}