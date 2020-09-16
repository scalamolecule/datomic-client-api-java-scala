package datomicScala.anomaly

import java.util.{Map => jMap}
import clojure.lang.{ExceptionInfo, Keyword}
import datomic.Util.read
import scala.jdk.CollectionConverters._

trait AnomalyWrapper {


  def anomalyCat(e: ExceptionInfo): String = {
    e.data.entryAt(read(":cognitect.anomalies/category"))
      .getValue.asInstanceOf[Keyword].getName
  }

  def anomalyMsg(e: ExceptionInfo): String = {
    e.data.entryAt(read(":cognitect.anomalies/message")).getValue.toString
  }

  def httpResult(e: ExceptionInfo): Map[String, Any] = {
    e.data.entryAt(read(":http-result")).getValue.asInstanceOf[jMap[_, _]]
      .asScala.toMap.map {
      case (k: Keyword, v: jMap[_, _]) => (k.getName, v.asScala.toMap)
      case (k: Keyword, v)             => (k.getName, v)
      case (k, v)                      => (k.toString, v)
    }
  }

  def anomalyValue(e: ExceptionInfo, key: String): Any = {
    e.data.entryAt(read(s":cognitect.anomalies/$key")).getValue match {
      case kw: Keyword => kw.asInstanceOf[Keyword].getName
      case s: String   => s
      case other       => other
    }
  }

  def catchAnomaly[T](body: => T): T = {
    try {
      // run test code
      body
    } catch {
      case e: ExceptionInfo =>
        val ae = anomalyCat(e) match {
          case "not-found" => NotFound(anomalyMsg(e))
          case "forbidden" => Forbidden(httpResult(e))
          case other       => new RuntimeException("Unexpected anomaly: " + other)
        }
        throw ae

      case e: Throwable => throw e
    }
  }
}
