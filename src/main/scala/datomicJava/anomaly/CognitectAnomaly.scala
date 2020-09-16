package datomicJava.anomaly
import java.util.{Map => jMap}

sealed trait CognitectAnomaly extends RuntimeException {
  val msg: String
}

case class Forbidden(httpRequest: jMap[String, Any]) extends CognitectAnomaly{
  val msg = "forbidden"
}

case class NotFound(msg: String) extends CognitectAnomaly

// todo: when and how are these used? Can we make some tests using them?
case class Unavailable(msg: String) extends CognitectAnomaly
case class Interrupted(msg: String) extends CognitectAnomaly
case class Incorrect(msg: String) extends CognitectAnomaly
case class Unsupported(msg: String) extends CognitectAnomaly
case class Conflict(msg: String) extends CognitectAnomaly
case class Fault(msg: String) extends CognitectAnomaly
case class Busy(msg: String) extends CognitectAnomaly
