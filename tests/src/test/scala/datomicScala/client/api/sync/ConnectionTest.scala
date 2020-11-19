package datomicScala.client.api.sync

import datomic.Util
import datomic.Util.{list, read}
import datomicClojure.ErrorMsg
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

    // Limit -1 sets no-limit
    // (necessary for Peer Server datom accumulation exceeding default 1000)

    val iterable: Iterable[(Long, Iterable[Datom])] = conn.txRange(limit = -1)
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

    val array: Array[(Long, Array[Datom])] = conn.txRangeArray(limit = -1)
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
  }

  // (`withDb` and `widh` are tested in DbTest...)
}
