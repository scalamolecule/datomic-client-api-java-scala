package datomicJavaScala.client.api.sync


object ClientProvider {
  var system  : String     = "Not set yet. Can be: dev-local / peer-server / cloud"
  var client  : Client     = null // set in setup
  var conn    : Connection = null // set in setup
  var txReport: TxReport   = null // set in setup
}
