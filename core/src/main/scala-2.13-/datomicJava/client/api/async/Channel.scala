package datomicJava.client.api.async

import clojure.lang.PersistentArrayMap
import datomic.Util._
import datomicClient._
import datomicClient.anomaly.{AnomalyWrapper, CognitectAnomaly}
import datomicJava.client.api.async

// Mock wrapper around clojure.core.async.ManyToManyChannel
// To be properly implemented by consuming language (java/scala)
case class Channel[T](
  channelOrInternal: AnyRef,
  transform: Option[AnyRef => T] = None
) extends ClojureBridge with AnomalyWrapper {


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
            Left(anomaly(anomalyMap))

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
}
