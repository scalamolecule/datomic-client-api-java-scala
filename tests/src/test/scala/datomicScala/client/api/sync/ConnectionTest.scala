package datomicScala.client.api.sync

import datomic.Util
import datomic.Util.{list, read}
import datomicScala.Spec
import datomicScala.client.api.Datom


class ConnectionTest extends Spec {

  "db" in new Setup {
    // Test if repeated calls do conn.db returns the same db value (/object)
    val db = conn.db

    if (isDevLocal) {
      // Dev-local connection returns same database object
      db === conn.db
    } else {
      // Peer Server connection returns new database object
      db !== conn.db
    }
  }


  "sync" in new Setup {

    if (isDevLocal) {
      // Same db object
      conn.sync(tAfter) === dbAfter
    } else {
      // todo? Does sync call need to create a new db object
      //  or could it be memoized/cached?
      // New db object
      conn.sync(tAfter) !== dbAfter
    }
  }


  "transact" in new Setup {
    films(conn.db) === threeFilms
    conn.transact(list(
      Util.map(
        read(":movie/title"), "Film 4",
      )
    ))
    films(conn.db) === fourFilms

    // Applying empty list of stmts returns empty TxReport without touching the db
    conn.transact(list()) === TxReport(Util.map())
  }


  "txRange" in new Setup {

    // Getting all transactions (!) -----------------------------------

    val iterable: Iterable[(Long, Iterable[Datom])] = conn.txRange()

    // OBS: if this doesn't pass, try restarting the peer server
    iterable.last._1 === tAfter
    iterable.last._2.toList === List(
      Datom(txAfter, 50, txInstAfter, txAfter, true),
      Datom(e1, a1, "The Goonies", txAfter, true),
      Datom(e1, a2, "action/adventure", txAfter, true),
      Datom(e1, a3, 1985, txAfter, true),
      Datom(e2, a1, "Commando", txAfter, true),
      Datom(e2, a2, "thriller/action", txAfter, true),
      Datom(e2, a3, 1985, txAfter, true),
      Datom(e3, a1, "Repo Man", txAfter, true),
      Datom(e3, a2, "punk dystopia", txAfter, true),
      Datom(e3, a3, 1984, txAfter, true)
    )

    val array: Array[(Long, Array[Datom])] = conn.txRangeArray()
    array.last._1 === tAfter
    array.last._2 === Array(
      Datom(txAfter, 50, txInstAfter, txAfter, true),
      Datom(e1, a1, "The Goonies", txAfter, true),
      Datom(e1, a2, "action/adventure", txAfter, true),
      Datom(e1, a3, 1985, txAfter, true),
      Datom(e2, a1, "Commando", txAfter, true),
      Datom(e2, a2, "thriller/action", txAfter, true),
      Datom(e2, a3, 1985, txAfter, true),
      Datom(e3, a1, "Repo Man", txAfter, true),
      Datom(e3, a2, "punk dystopia", txAfter, true),
      Datom(e3, a3, 1984, txAfter, true)
    )

    // Get range from timePointStart to timePointEnd ------------------

    val txReport4 = conn.transact(film4)

    Thread.sleep(5) // Make sure that date's don't share same ms

    val txReport5 = conn.transact(film5)

    Thread.sleep(5) // Make sure that date's don't share same ms

    val txReport6 = conn.transact(film6)

    val tx4 = txReport4.tx
    val tx5 = txReport5.tx
    val tx6 = txReport6.tx

    val t4 = txReport4.t
    val t5 = txReport5.t
    val t6 = txReport6.t

    val txInstant4 = txReport4.txInst
    val txInstant6 = txReport6.txInst


    def checkRange(
      timePointStart: Option[Any],
      timePointEnd: Option[Any],
      expected: List[TxReport]
    ): Unit = {
      conn.txRange(timePointStart, timePointEnd).toList.map {
        case (t, datoms) => (t, datoms.toList)
      } === expected.map { txReport =>
        (txReport.t, txReport.txData.toArray.toList.asInstanceOf[List[Datom]])
      }
    }

    def checkUntil(
      timePointEnd: Any,
      expectedLastT: Long
    ): Unit = {
      conn.txRange(None, Some(timePointEnd)).map(_._1).last === expectedLastT
    }

    // timePointStart is after timePointEnd - returns Nil
    checkRange(Some(tx5), Some(tx4), Nil)

    // timePointEnd is exclusive, so tx4 is not considered
    checkRange(Some(tx4), Some(tx4), Nil)

    // tx 4
    checkRange(Some(tx4), Some(tx5), List(txReport4))

    // tx 4-5
    checkRange(Some(tx4), Some(tx6), List(txReport4, txReport5))

    // To get until the last tx, set timePointEnd to None
    checkRange(Some(tx4), None, List(txReport4, txReport5, txReport6))
    checkRange(Some(tx5), None, List(txReport5, txReport6))
    checkRange(Some(tx6), None, List(txReport6))

    // To get from first tx, set timePointStart to None
    checkUntil(txAfter, tBefore)
    checkUntil(tx4, tAfter)
    checkUntil(tx5, t4)
    checkUntil(tx6, t5)

    // Getting txRange with t/tx/txInst

    // Using time t
    checkRange(Some(t4), Some(t6), List(txReport4, txReport5))

    // Using transaction id
    checkRange(Some(t4), Some(t6), List(txReport4, txReport5))


//    Thread
    // Using Date
    checkRange(Some(txInstant4), Some(txInstant6), List(txReport4, txReport5))
  }

  // (`withDb` and `widh` are tested in DbTest...)
}
