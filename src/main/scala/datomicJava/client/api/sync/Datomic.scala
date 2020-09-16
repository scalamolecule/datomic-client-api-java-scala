package datomicJava.client.api.sync

import java.util.stream.{Stream => jStream}
import java.util.{Collection => jCollection, List => jList, Map => jMap}
import com.amazonaws.auth.AWSCredentialsProviderChain
import datomicClojure.{ClojureBridge, Invoke}
import datomicJava.anomaly.AnomalyWrapper

object Datomic extends ClojureBridge with AnomalyWrapper {

  require("datomic.client.api")

  // Providing AWSCredentialsProviderChain
  def clientCloud(
    region: String,
    system: String,
    endpoint: String,
    credsProvider: AWSCredentialsProviderChain,
    proxyPort: Int
  ): Client = Client(
    false,
    Invoke.clientCloudAWS(region, system, endpoint, credsProvider, proxyPort)
  )

  // Providing creds-profile name
  def clientCloud(
    region: String,
    system: String,
    endpoint: String,
    credsProfile: String,
    proxyPort: Int
  ): Client = Client(
    false,
    Invoke.clientCloudCredsProfile(region, system, endpoint, credsProfile, proxyPort)
  )


  def clientDevLocal(
    system: String,
  ): Client = clientDevLocal(system, "")

  def clientDevLocal(
    system: String,
    storageDir: String // overrides :storage-dir in ~/.datomic/dev-local.edn
  ): Client = Client(
    false,
    Invoke.clientDevLocal(system, storageDir)
  )


  def clientPeerServer(
    accessKey: String,
    secret: String,
    endpoint: String,
  ): Client = clientPeerServer(accessKey, secret, endpoint, false)

  def clientPeerServer(
    accessKey: String,
    secret: String,
    endpoint: String,
    validateHostnames: Boolean
  ): Client = Client(
    true,
    Invoke.clientPeerServer(accessKey, secret, endpoint, validateHostnames)
  )


  // Query as data structure or String + optional :offset, :limit, :timeout params
  // (see tests)
  def q(argMap: jMap[_, _]): jCollection[jList[AnyRef]] = catchAnomaly {
    Invoke.q(argMap)
  }

  // Spelling out multiple arities to satisfy Scala/Java compatibility

  // Query as data structure
  def q(query: jList[_], db: Db): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb))
  def q(query: jList[_], db: Db, a: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a))
  def q(query: jList[_], db: Db, a: Any, b: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d, e))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d, e, f))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d, e, f, g))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d, e, f, g, h))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any, u: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t, u))
  def q(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any, u: Any, v: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t, u, v))

  // Query as String
  def q(query: String, db: Db): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb))
  def q(query: String, db: Db, a: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a))
  def q(query: String, db: Db, a: Any, b: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b))
  def q(query: String, db: Db, a: Any, b: Any, c: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d, e))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d, e, f))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d, e, f, g))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d, e, f, g, h))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d, e, f, g, h, i))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any, u: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t, u))
  def q(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any, u: Any, v: Any): jCollection[jList[AnyRef]] = catchAnomaly(Invoke.q(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t, u, v))


  // Query as data structure or String + optional :offset, :limit, :timeout params
  // (see tests)
  def qseq(map: jMap[_, _]): jStream[_] = catchAnomaly{
      Invoke.qseq(map)
  }

  // Query as data structure
  def qseq(query: jList[_], db: Db): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb))
  def qseq(query: jList[_], db: Db, a: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a))
  def qseq(query: jList[_], db: Db, a: Any, b: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d, e))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d, e, f))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d, e, f, g))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d, e, f, g, h))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any, u: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t, u))
  def qseq(query: jList[_], db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any, u: Any, v: Any): jStream[_] = catchAnomaly(Invoke.qseq(edn(query), db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t, u, v))


  // Query as String
  def qseq(query: String, db: Db): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb))
  def qseq(query: String, db: Db, a: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a))
  def qseq(query: String, db: Db, a: Any, b: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d, e))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d, e, f))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d, e, f, g))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d, e, f, g, h))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d, e, f, g, h, i))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any, u: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t, u))
  def qseq(query: String, db: Db, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any, u: Any, v: Any): jStream[_] = catchAnomaly(Invoke.qseq(query, db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t, u, v))

}
