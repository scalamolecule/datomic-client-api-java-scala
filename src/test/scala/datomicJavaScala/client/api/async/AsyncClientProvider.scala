package datomicJavaScala.client.api.async

import datomicJavaScala.client.api.sync.TxReport


object AsyncClientProvider {
  var system  : String          = "Not set yet. Can be: dev-local / peer-server / cloud"
  var client  : AsyncClient     = null // set in setup
  var conn    : AsyncConnection = null // set in setup
  var txReport: AsyncTxReport   = null // set in setup
}
