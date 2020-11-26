package datomicClient.anomaly

import java.util
import java.util.{Map => jMap}
import clojure.lang.{ExceptionInfo, Keyword, PersistentArrayMap}
import datomic.Util.{map, read}

trait AnomalyWrapper {

  def catchAnomaly[T](codeToRun: => T): T = {
    try {
      codeToRun
    } catch {
      case e: ExceptionInfo => throw anomaly(e)
      case e: Throwable     => throw e
    }
  }


  def anomaly(e: ExceptionInfo): CognitectAnomaly = {
    val cat = {
      val key = read(":cognitect.anomalies/category")
      if (e.data.containsKey(key))
        e.data.entryAt(key).getValue.asInstanceOf[Keyword].getName
      else
        "unknown"
    }

    def msg: String = {
      val key = read(":cognitect.anomalies/message")
      if (e.data.containsKey(key))
        e.data.entryAt(key).getValue.toString
      else
        e.getMessage
    }

    cat match {
      case "forbidden"   => {
        val httpResult = new util.HashMap[String, Any]()
        e.data.entryAt(read(":http-result"))
          .getValue.asInstanceOf[jMap[_, _]].forEach {
          case (k: Keyword, v: jMap[_, _]) => httpResult.put(k.getName, v)
          case (k: Keyword, v)             => httpResult.put(k.getName, v)
          case (k, v)                      => httpResult.put(k.toString, v)
        }
        Forbidden(httpResult)
      }
      case "not-found"   => NotFound(msg)
      case "unavailable" => Unavailable(msg)
      case "interrupted" => Interrupted(msg)
      case "incorrect"   => Incorrect(msg)
      case "unsupported" => Unsupported(msg)
      case "conflict"    => Conflict(msg)
      case "fault"       => Fault(msg)
      case "busy"        => Busy(msg)
      case _             => throw new RuntimeException(
        "Unexpected non-anomaly exception: " + e)
    }
  }


  def anomaly(anomalyMap: PersistentArrayMap): CognitectAnomaly = {
    val cat: String = anomalyMap.get(
      read(":cognitect.anomalies/category")
    ).asInstanceOf[Keyword].getName

    lazy val msg: String = anomalyMap.get(
      read(":cognitect.anomalies/message")
    ).toString

    cat match {
      case "forbidden"   => {
        val httpResult = map().asInstanceOf[jMap[String, Any]]
        anomalyMap.entryAt(read(":http-result")).getValue.asInstanceOf[jMap[_, _]].forEach {
          case (k: Keyword, v: jMap[_, _]) => httpResult.put(k.getName, v)
          case (k: Keyword, v)             => httpResult.put(k.getName, v)
          case (k, v)                      => httpResult.put(k.toString, v)
        }
        Forbidden(httpResult)
      }
      case "not-found"   => NotFound(msg)
      case "unavailable" => Unavailable(msg)
      case "interrupted" => Interrupted(msg)
      case "incorrect"   => Incorrect(msg)
      case "unsupported" => Unsupported(msg)
      case "conflict"    => Conflict(msg)
      case "fault"       => Fault(msg)
      case "busy"        => Busy(msg)
      case _             => throw new IllegalArgumentException(
        "Unexpected Anomaly:\n" + anomalyMap
      )
    }
  }
}
