package datomicClient

import java.util.{Date, Map => jMap}
import clojure.lang.ILookup
import datomic.Peer
import datomic.Util.read
import datomicScala.client.api.sync.Db


class DbLookup(
  datomicDb: AnyRef,
  sinceTimePoint: Option[(Long, Long, Date)] = None
) extends ClojureBridge {

  lazy protected val isDevLocal = datomicDb.isInstanceOf[clojure.lang.IPersistentMap]

  def dbName: String = valAt[String](if (isDevLocal) ":id" else ":db-name").get

  def t: Long = valAt[Long](":t").getOrElse(0L)

  def asOfT: Long = {
    if (isDevLocal) {
      valAt[Long](":as-of").getOrElse(0L)
    } else {
      datomicDb.asInstanceOf[clojure.lang.ILookup].valAt(read(":as-of"), "err") match {
        case d: Date =>
          // Find datom from txInstant value (within d to d2 range)
          val d2 = Date.from(d.toInstant.plusSeconds(1))
          val it = Invoke.indexRange(datomicDb, ":db/txInstant", Some(d), Some(d2))
            .asInstanceOf[java.lang.Iterable[_]].iterator()

          if (it.hasNext) {
            it.next().asInstanceOf[ILookup].valAt(read(":e"), "err") match {
              case "err" => throw new RuntimeException("Unexpected missing e value for datom.")
              case v     =>
                Peer.toT(v.toString.toLong)
            }
          } else {
            throw new RuntimeException(
              "Unexpectedly couldn't find datom with txInstant " + d.toInstant
            )
          }
        case "err"   => 0L
        case tx      => Peer.toT(tx)
      }
    }
  }

  def asOfTxInst: Date = {
    val timePoint = datomicDb.asInstanceOf[clojure.lang.ILookup].valAt(read(":as-of"), "err")
    timePoint match {
      case d: Date => d
      case "err"   => null
      case t       =>
        val tx   = Peer.toTx(t.asInstanceOf[Long])
        val inst = Db(datomicDb).pull("[:db/txInstant]", tx)
        if (inst == null) {
          null
        } else {
          inst.asInstanceOf[clojure.lang.PersistentArrayMap]
            .get(read(":db/txInstant")).asInstanceOf[Date] match {
            // Beginning of time considered null
            case d if d == new Date(0) => null
            case d                     => d
          }
        }
    }
  }

  def sinceT: Long = if (isDevLocal) {
    valAt[Long](":since").getOrElse(0L)
  } else {
    sinceTimePoint.fold(0L) { case (t, _, _) => t }
  }

  // Cached from original db
  def sinceTxInst: Date = sinceTimePoint match {
    case Some((_, _, txInst)) => txInst
    case None                 => null
  }

  def isHistory: Boolean = if (isDevLocal) {
    valAt[Boolean](":history?").getOrElse(false)
  } else {
    valAt[Boolean](":history").getOrElse(false)
  }

  private def valAt[T](key: String): Option[T] = {
    // dev-local and peer-server have different implementations
    datomicDb.asInstanceOf[clojure.lang.ILookup].valAt(read(key), "err") match {
      case null  => None // returned by dev-local
      case "err" => None // returned by Peer Server
      case v     => Some(v.asInstanceOf[T])
    }
  }

  protected def extractSinceTimePoint(tOrTx: Long): Option[(Long, Long, Date)] = {
    // todo: Blocking... hmm
    val t  = Peer.toT(tOrTx.asInstanceOf[Long])
    val tx = Peer.toTx(tOrTx.asInstanceOf[Long]).asInstanceOf[Long]
    val d  = Invoke.pull(datomicDb, "[:db/txInstant]", tx).asInstanceOf[jMap[_, _]]
      .get(read(":db/txInstant")).asInstanceOf[Date]
    Some((t, tx, d))
  }

  protected def extractSinceTimePoint(d: Date): Option[(Long, Long, Date)] = {
    // todo: Blocking... hmm
    val d2      = Date.from(d.toInstant.plusSeconds(1))
    val it      = Invoke.indexRange(datomicDb, ":db/txInstant", Some(d), Some(d2))
      .asInstanceOf[java.lang.Iterable[_]].iterator()
    val (t, tx) = if (it.hasNext) {
      it.next().asInstanceOf[ILookup].valAt(read(":e"), "err") match {
        case "err" => throw new RuntimeException("Unexpected missing e value for datom.")
        case v     =>
          val n = v.toString.toLong
          (Peer.toT(n), Peer.toTx(n).asInstanceOf[Long])
      }
    } else {
      throw new RuntimeException(
        "Unexpectedly couldn't find datom with txInstant " + d.toInstant
      )
    }
    Some((t, tx, d))
  }
}
