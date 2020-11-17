package datomicScala.client.api.async

import datomicScala.SpecAsync


class AsyncTxReportTest extends SpecAsync {
  sequential


  "4 txReport ops" in new AsyncSetup {

    films(filmDataTx.dbBefore) == Nil
    films(filmDataTx.dbAfter) == threeFilms

    // Tx datom + 3 entities * 3 attributes transacted
    filmDataTx.txData.count === 1 + 3 * 3

    if (isDevLocal) {
      // No temp ids created with dev-local setup
      filmDataTx.tempIds.size === 0
    } else {
      filmDataTx.tempIds.size === 3
    }
  }
}