package datomicClient.anomaly

import java.util.{Map => jMap}

abstract class CognitectAnomaly(msg: String) extends RuntimeException(msg)

case class Forbidden(httpRequest: jMap[String, Any]) extends CognitectAnomaly("forbidden")

case class NotFound(msg: String) extends CognitectAnomaly(msg)
case class Unavailable(msg: String) extends CognitectAnomaly(msg)
case class Interrupted(msg: String) extends CognitectAnomaly(msg)
case class Incorrect(msg: String) extends CognitectAnomaly(msg)
case class Unsupported(msg: String) extends CognitectAnomaly(msg)
case class Conflict(msg: String) extends CognitectAnomaly(msg)
case class Fault(msg: String) extends CognitectAnomaly(msg)
case class Busy(msg: String) extends CognitectAnomaly(msg)
