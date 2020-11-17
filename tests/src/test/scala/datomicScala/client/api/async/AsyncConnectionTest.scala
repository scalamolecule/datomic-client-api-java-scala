package datomicScala.client.api.async

import datomic.Util
import datomic.Util.{list, read}
import datomicClojure.ErrorMsg
import datomicScala.SpecAsync
import datomicScala.client.api.Datom


class AsyncConnectionTest extends SpecAsync {
  sequential


  // (same as sync version)
  "db" in new AsyncSetup {
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


  "sync" in new AsyncSetup {

    // Db value the same
    conn.sync(tAfter).equals(dbAfter)

    // Db object identity
    if (isDevLocal) {
      // Same db object
      conn.sync(tAfter) === dbAfter
    } else {
      // Db object copy
      conn.sync(tAfter) !== dbAfter
    }
  }


  "transact" in new AsyncSetup {
    films(conn.db) === threeFilms
    waitFor(conn.transact(list(
      Util.map(
        read(":movie/title"), "Film 4",
      )
    ))).toOption.get
    films(conn.db) === fourFilms

    waitFor(conn.transact(list())) must throwA(
      new IllegalArgumentException(ErrorMsg.transact)
    )
  }


  "txRange" in new AsyncSetup {

    // Limit -1 sets no-limit
    // (necessary for Peer Server datom accumulation exceeding default 1000)

    val iterable: Iterable[(Long, Iterable[Datom])] = waitFor(conn.txRange(limit = -1)).toOption.get
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

    val array: Array[(Long, Array[Datom])] = waitFor(conn.txRangeArray(limit = -1)).toOption.get
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

  // (`withDb` and `widh` are tested in AsyncDbTest...)
}
