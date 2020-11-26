package datomicClient

object ErrorMsg {

  def administerSystem: String =
    "administerSystem is not available with a client running against a Peer Server."

  def createDatabase(dbName: String): String =
    s"""createDatabase is not available with a client running against a Peer Server.
       |Please create a database with the Peer class instead:
       |Peer.createDatabase("datomic:<free/dev/pro>://<host>:<port>/$dbName")""".stripMargin


  def deleteDatabase(dbName: String): String =
    s"""deleteDatabase is not available with a client running against a Peer Server.
       |Please delete a database with the Peer class instead:
       |Peer.deleteDatabase("datomic:<free/dev/pro>://<host>:<port>/$dbName")""".stripMargin


  def indexPull: String = "Index can only be :avet or :aevt"

  def zeroNeg: String = "Number can't be 0 or negative"

  def limit: String = "Limit can only be a positive number or -1 for 'no limit'"
}
