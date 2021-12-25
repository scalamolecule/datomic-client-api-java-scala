package datomicScala.client.api.async

//import cats.effect.{ContextShift, IO}
import cats.effect.IO
import clojure.lang.PersistentArrayMap
import datomic.Util._
import datomicClient._
import datomicClient.anomaly.{AnomalyWrapper, CognitectAnomaly}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


case class Channel[T](
  channelOrInternal: AnyRef,
  transform: Option[AnyRef => T] = None
) extends ClojureBridge with AnomalyWrapper {

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
            Left(anomaly(anomalyMap)) #:: LazyList.empty

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


  // Alternative fs2 Stream implementation
  // This allows the first chunk to be lazy too.
  // Usage is flexible but verbose

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
              Some(Left(anomaly(anomalyMap)))

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
