package datomicScala.client.api.sync

import datomicScala.SyncSpec
import datomicScala.client.api.Datom


class ConnectionTest extends SyncSpec {
  sequential


  "txRange" in new Setup {
    val txRange2: Iterable[(Long, Iterable[Datom])] = conn.txRange()
    val l                                           = txRange2.toList
    l.last._1 === tAfter
    l.last._2.toList === List(
      Datom(txIdAfter, 50, txInst, txIdAfter, true),
      Datom(e1, a1, "The Goonies", txIdAfter, true),
      Datom(e1, a2, "action/adventure", txIdAfter, true),
      Datom(e1, a3, 1985, txIdAfter, true),
      Datom(e2, a1, "Commando", txIdAfter, true),
      Datom(e2, a2, "thriller/action", txIdAfter, true),
      Datom(e2, a3, 1985, txIdAfter, true),
      Datom(e3, a1, "Repo Man", txIdAfter, true),
      Datom(e3, a2, "punk dystopia", txIdAfter, true),
      Datom(e3, a3, 1984, txIdAfter, true)
    )

    val txRange: Array[(Long, Array[Datom])] = conn.txRangeArray()
    txRange.last._1 === tAfter
    txRange.last._2 === Array(
      Datom(txIdAfter, 50, txInst, txIdAfter, true),
      Datom(e1, a1, "The Goonies", txIdAfter, true),
      Datom(e1, a2, "action/adventure", txIdAfter, true),
      Datom(e1, a3, 1985, txIdAfter, true),
      Datom(e2, a1, "Commando", txIdAfter, true),
      Datom(e2, a2, "thriller/action", txIdAfter, true),
      Datom(e2, a3, 1985, txIdAfter, true),
      Datom(e3, a1, "Repo Man", txIdAfter, true),
      Datom(e3, a2, "punk dystopia", txIdAfter, true),
      Datom(e3, a3, 1984, txIdAfter, true)
    )
  }


  // (`withDb` and `widh` are tested in DbTest...)


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
}
