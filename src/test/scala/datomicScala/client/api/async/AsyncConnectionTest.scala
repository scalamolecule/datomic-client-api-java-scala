package datomicScala.client.api.async

import datomicScala.client.api.Datom


class AsyncConnectionTest extends AsyncSetupSpec {
  sequential


  "txRange" in new AsyncSetup {
    // For some reason we can't compare (7, Array(...)) but separately goes fine:

    // todo: manually selecting last tx - until all txs are returned
    // (see AsyncConnection.txRange comments)
    val txRange = conn.txRange(offset = 7, limit = 1).realize
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
}
