package datomicScala.client.api.async


class AsyncTxReportTest extends AsyncSetupSpec {
  sequential

  "4 txReport ops" in new AsyncSetup {

    films(txReport.dbBefore) == Nil
    films(txReport.dbAfter) == threeFilms

    // Tx datom + 3 entities * 3 attributes transacted
    txReport.txData.count === 1 + 3 * 3

    if (isDevLocal) {
      // No temp ids created with dev-local setup
      txReport.tempIds.size === 0
    } else {
      txReport.tempIds.size === 3
    }
  }
}