package datomicJava.client.api.async

import java.util.{Map => jMap}
import clojure.lang.{Keyword, PersistentArrayMap}
import datomic.Util._
import datomicClojure.ClojureBridge
import datomicJava._
import datomicJava.client.api.async

// Mock wrapper around clojure.core.async.ManyToManyChannel
// To be properly implemented by consuming language (java/scala)
case class Channel[T](
  channelOrInternal: AnyRef,
  transform: Option[AnyRef => T] = None
) extends ClojureBridge {


  def chunk: Either[CognitectAnomaly, T] = {
    // Consume channel head
    channelOrInternal match {
      case channel: clojure.lang.IType =>
        coreAsyncFn("<!!").invoke(channel) match {
          // Empty result
          case null =>
            Right(null.asInstanceOf[T])

          // Anomaly
          case anomalyMap: PersistentArrayMap
            if anomalyMap.containsKey(read(":cognitect.anomalies/category")) =>
            Left(getAnomaly(anomalyMap))

          // Chunks with type transformation
          case chunk if transform.nonEmpty =>
            async.Right(transform.get(chunk))

          // Chunks casted
          case chunk =>
            async.Right(chunk.asInstanceOf[T])
        }

      // Internal types like TxReport etc.
      case internal =>
        async.Right(internal.asInstanceOf[T])
    }
  }

  private def getAnomaly(anomalyMap: PersistentArrayMap): CognitectAnomaly = {
    val cat = anomalyMap.get(
      read(":cognitect.anomalies/category")
    ).asInstanceOf[Keyword].getName

    val msg = anomalyMap.get(
      read(":cognitect.anomalies/message")
    ).toString

    cat match {
      case "not-found"   => NotFound(msg)
      case "forbidden"   => Forbidden(httpResult(anomalyMap))
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


  def httpResult(data: PersistentArrayMap): jMap[String, Any] = {
    val javaMap = map().asInstanceOf[jMap[String, Any]]
    val res = data.entryAt(read(":http-result")).getValue.asInstanceOf[jMap[_, _]]
    res.forEach {
      case (k: Keyword, v: jMap[_, _]) => javaMap.put(k.getName, v)
      case (k: Keyword, v)             => javaMap.put(k.getName, v)
      case (k, v)                      => javaMap.put(k.toString, v)
    }
    javaMap
  }
}
