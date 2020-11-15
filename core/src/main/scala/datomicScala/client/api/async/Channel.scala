package datomicScala.client.api.async

import java.util.{Map => jMap}
import cats.effect.{ContextShift, IO}
import clojure.lang.{IPersistentMap, Keyword, PersistentArrayMap}
import datomic.Util._
import datomicClojure.ClojureBridge
import datomicScala._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters._


case class Channel[T](
  channelOrInternal: AnyRef,
  transform: Option[AnyRef => T] = None
) extends ClojureBridge {

  // Recursively and lazily extract data (except first chunk) from Clojure Channel
  def lazyList: LazyList[Either[CognitectAnomaly, T]] = {
    // Consume channel head
    channelOrInternal match {
      case channel: clojure.lang.IType =>
        // Initial invocation (head of LazyList) is not lazy in this implementation
        // Use qStream for a fully lazy fs2 Stream implementation (see below)
        coreAsyncFn("<!!").invoke(channel) match {
          // Empty result
          case null =>
            LazyList.empty

          // Anomaly
          case anomalyMap: PersistentArrayMap
            if anomalyMap.containsKey(read(":cognitect.anomalies/category")) =>
            Left(getAnomaly(anomalyMap)) #:: LazyList.empty

          // Chunks with type transformation
          case chunk if transform.nonEmpty =>
            Right(transform.get(chunk)) #:: lazyList

          // Chunks casted
          case chunk =>
            Right(chunk.asInstanceOf[T]) #:: lazyList
        }

      // Internal types like TxReport etc.
      case internal =>
        Right(internal.asInstanceOf[T]) #:: LazyList.empty
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
      case "not-found"   => NotFound(msg, null)
      case "forbidden"   => Forbidden(httpResult(anomalyMap), null)
      case "unavailable" => Unavailable(msg, null)
      case "interrupted" => Interrupted(msg, null)
      case "incorrect"   => Incorrect(msg, null)
      case "unsupported" => Unsupported(msg, null)
      case "conflict"    => Conflict(msg, null)
      case "fault"       => Fault(msg, null)
      case "busy"        => Busy(msg, null)
      case _             => throw new IllegalArgumentException(
        "Unexpected Anomaly:\n" + anomalyMap
      )
    }
  }


  def httpResult(data: IPersistentMap): Map[String, Any] = {
    data.entryAt(read(":http-result")).getValue.asInstanceOf[jMap[_, _]]
      .asScala.toMap.map {
      case (k: Keyword, v: jMap[_, _]) => (k.getName, v.asScala.toMap)
      case (k: Keyword, v)             => (k.getName, v)
      case (k, v)                      => (k.toString, v)
    }
  }


  // Alternative fs2 Stream implementation
  // This allows the first chunk to be lazy too.
  // Usage is flexible but verbose

  implicit val ioContextShift: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.Implicits.global)

  private def getChunk: Future[Option[Either[CognitectAnomaly, T]]] = {
    Future {
      channelOrInternal match {
        case channel: clojure.lang.IType =>
          coreAsyncFn("<!!").invoke(channel) match {
            // Empty result
            case null => None

            // Anomaly
            case anomalyMap: PersistentArrayMap
              if anomalyMap.containsKey(read(":cognitect.anomalies/category")) =>
              Some(Left(getAnomaly(anomalyMap)))

            // Chunks with type transformation
            case chunk if transform.nonEmpty =>
              Some(Right(transform.get(chunk)))

            // Chunks casted
            case chunk =>
              Some(Right(chunk.asInstanceOf[T]))
          }

        // Internal types like TxReport etc.
        case internal =>
          Some(Right(internal.asInstanceOf[T]))
      }
    }
  }

  def myTerminatedStream: fs2.Stream[IO, Either[CognitectAnomaly, T]] = {
    val typedChunks = IO.fromFuture(IO(getChunk))
    fs2.Stream.eval(typedChunks).repeat.unNoneTerminate
  }
}
