package datomicJavaScala.client.api.async

import clojure.lang.PersistentArrayMap
import datomicJavaScala.util.ClojureBridge
import datomic.Util._

// Mock wrapper around clojure.core.async.ManyToManyChannel
// To be properly implemented by consuming language (java/scala)
class Channel[T](
  channelOrInternal: AnyRef,
  transform: Option[AnyRef => T] = None
) extends ClojureBridge {

  // Get value(s)/execute blocking op from channel
  def realize: T = channelOrInternal match {
    case channel: clojure.lang.IType =>
      coreAsyncFn("<!!").invoke(channel) match {

        // Empty result
        case null => null.asInstanceOf[T]

        // Simply throw exception if anomaly is on the channel
        case anomaly: PersistentArrayMap
          if anomaly.containsKey(read(":cognitect.anomalies/category")) =>
          throw new IllegalArgumentException("Anomaly found:\n" + anomaly)

        case channelValue if transform.nonEmpty => transform.get(channelValue)
        case channelValue                       => channelValue.asInstanceOf[T]
      }

    // Internal types like TxReport etc.
    case internal => internal.asInstanceOf[T]
  }
}
