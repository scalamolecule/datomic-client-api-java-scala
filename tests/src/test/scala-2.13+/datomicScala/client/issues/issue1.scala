//package datomicScala.client.issues
//
//import java.util.stream
//import java.util.stream.{Stream => jStream}
//import cats.effect.IO
//import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
//import datomic.Util
//import datomic.Util._
//import datomicClient.anomaly.{AnomalyWrapper, CognitectAnomaly, Forbidden, NotFound}
//import datomicScala.SpecAsync
//import datomicScala.client.api.async.AsyncDatomic
//import scala.concurrent.Future
//import scala.jdk.StreamConverters._
//
//
//class issue1 extends SpecAsync with AnomalyWrapper{
//
////  "create client" >> {
////    import clojure.lang.Keyword
////    import clojure.lang.PersistentHashMap
////    import com.sun.tools.javac.util.List
////    import datomic.Util
////    val sclient = AsyncDatomic.clientPeerServer("datomic", "datomic", "localhost:8998")
////    val fcon    = sclient.connect("dev-entity-system")
////    val either  = fcon.get
////    val right   = either.asInstanceOf[Nothing]
////    val con     = right.right_value.asInstanceOf[Nothing]
////    val sdb     = con.db
////    val qr      = AsyncDatomic.q(
////      PersistentHashMap.create(
////        Map.of(
////          Keyword.intern("query"),
////          Util.read("[:find ?roles :in $ :where [?e :LoadUser/roles2 " + ":LoadUserRole/R14][?e :LoadUser/roles2 ?roles]]"),
////          Keyword.intern("args"),
////          List.of(sdb.datomicDb)
////        )
////      ).asInstanceOf[PersistentHashMap]
////    )
////  }
//
//  "create client" >> {
//    import clojure.lang.Keyword
//    import clojure.lang.PersistentHashMap
//    import com.sun.tools.javac.util.List
//    import datomic.Util
//
//    val sclient = AsyncDatomic.clientPeerServer("datomic", "datomic", "localhost:8998")
//    val fcon    = sclient.connect("dev-entity-system")
//    val either  = fcon.get
//    val right   = either.asInstanceOf[Nothing]
//    val con     = right.right_value.asInstanceOf[Nothing]
//    val sdb     = con.db
//    val qr      = AsyncDatomic.q(
//      PersistentHashMap.create(
//        Map.of(
//          Keyword.intern("query"),
//          Util.read("[:find ?roles :in $ :where [?e :LoadUser/roles2 " + ":LoadUserRole/R14][?e :LoadUser/roles2 ?roles]]"),
//          Keyword.intern("args"),
//          List.of(sdb.datomicDb)
//        )
//      ).asInstanceOf[PersistentHashMap]
//    )
//  }
//
//}
