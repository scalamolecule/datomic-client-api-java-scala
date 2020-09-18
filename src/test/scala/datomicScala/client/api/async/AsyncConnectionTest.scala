package datomicScala.client.api.async

import datomic.Util
import datomic.Util.{list, read}
import datomicClojure.ErrorMsg
import datomicScala.SpecAsync
import datomicScala.client.api.Datom


class AsyncConnectionTest extends SpecAsync {
  sequential


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
    conn.sync(tAfter).realize.equals(dbAfter)

    // Db object identity
    if (isDevLocal) {
      // Same db object
      conn.sync(tAfter).realize === dbAfter
    } else {
      // Db object copy
      conn.sync(tAfter).realize !== dbAfter
    }
  }


  "transact" in new AsyncSetup {
    films(conn.db) === threeFilms
    conn.transact(list(
      Util.map(
        read(":movie/title"), "Film 4",
      )
    )).realize
    films(conn.db) === fourFilms

    conn.transact(list()).realize must throwA(
      new IllegalArgumentException(ErrorMsg.transact)
    )

    if (isDevLocal) resetDevLocalDb() else resetPeerServerDb()
  }


  "txRange" in new AsyncSetup {
    val iterable: Iterable[(Long, Iterable[Datom])] = conn.txRange().realize
    iterable.last._1 === tAfter
    iterable.last._2.toList === List(
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

    val array: Array[(Long, Array[Datom])] = conn.txRangeArray().realize
    array.last._1 === tAfter
    array.last._2 === Array(
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

  // (`withDb` and `widh` are tested in AsyncDbTest...)
}
