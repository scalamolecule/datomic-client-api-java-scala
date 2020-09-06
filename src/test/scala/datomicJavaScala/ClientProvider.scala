package datomicJavaScala

import datomicJavaScala.client.api.{Client, Connection, TxReport}


object ClientProvider {
  var system  : String     = "dev-local / peer-server / cloud"
  var client  : Client     = null
  var conn    : Connection = null
  var txReport: TxReport   = null
}
