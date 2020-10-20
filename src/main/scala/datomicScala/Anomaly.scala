package datomicScala

sealed trait CognitectAnomaly extends RuntimeException {
  val msg: String
}

case class Forbidden(httpRequest: Map[String, Any]) extends CognitectAnomaly {
  val msg = "forbidden"
}

case class NotFound(msg: String) extends CognitectAnomaly
case class Unavailable(msg: String) extends CognitectAnomaly
case class Interrupted(msg: String) extends CognitectAnomaly
case class Incorrect(msg: String) extends CognitectAnomaly
case class Unsupported(msg: String) extends CognitectAnomaly
case class Conflict(msg: String) extends CognitectAnomaly
case class Fault(msg: String) extends CognitectAnomaly
case class Busy(msg: String) extends CognitectAnomaly

/*
{
  :cognitect.anomalies/category :cognitect.anomalies/fault,
  :datomic.core.anomalies/exception #error {
    :cause ":db.error/unbound-query-variables Query is referencing unbound variables: #{?movie-titlex}"
    :data {
      :cognitect.anomalies/category :cognitect.anomalies/incorrect,
      :cognitect.anomalies/message "Query is referencing unbound variables: #{?movie-titlex}",
      :variables #{?movie-titlex}, :db/error :db.error/unbound-query-variables
    }
    :via
    [
      {
        :type com.google.common.util.concurrent.UncheckedExecutionException
        :message "clojure.lang.ExceptionInfo: :db.error/unbound-query-variables Query is referencing unbound variables: #{?movie-titlex} {:cognitect.anomalies/category :cognitect.anomalies/incorrect, :cognitect.anomalies/message \"Query is referencing unbound variables: #{?movie-titlex}\", :variables #{?movie-titlex}, :db/error :db.error/unbound-query-variables}"
        :at [com.google.common.cache.LocalCache$Segment get "LocalCache.java" 2049]
      }
      {
        :type clojure.lang.ExceptionInfo
        :message ":db.error/unbound-query-variables Query is referencing unbound variables: #{?movie-titlex}"
        :data {
          :cognitect.anomalies/category :cognitect.anomalies/incorrect,
          :cognitect.anomalies/message "Query is referencing unbound variables: #{?movie-titlex}",
          :variables #{?movie-titlex}, :db/error :db.error/unbound-query-variables
        }
        :at [datomic.core.error$raise invokeStatic "error.clj" 55]
      }
    ]
    :trace
    [[datomic.core.error$raise invokeStatic "error.clj" 55]
    [datomic.core.error$raise invoke "error.clj" 43]
    [datomic.core.error$arg invokeStatic "error.clj" 64]
    [datomic.core.error$arg invoke "error.clj" 59]
    [datomic.core.query$validate_query invokeStatic "query.clj" 311]
    [datomic.core.query$validate_query invoke "query.clj" 285]
    [datomic.core.query$parse_query invokeStatic "query.clj" 463]
    [datomic.core.query$parse_query invoke "query.clj" 451]
    [datomic.core.query$load_query invokeStatic "query.clj" 468]
    [datomic.core.query$load_query invoke "query.clj" 467]
    [datomic.core.impl.FnCacheLoader load "FnCacheLoader.java" 16]
    [com.google.common.cache.LocalCache$LoadingValueReference loadFuture "LocalCache.java" 3445]
    [com.google.common.cache.LocalCache$Segment loadSync "LocalCache.java" 2194]
    [com.google.common.cache.LocalCache$Segment lockedGetOrLoad "LocalCache.java" 2153]
    [com.google.common.cache.LocalCache$Segment get "LocalCache.java" 2043]
    [com.google.common.cache.LocalCache get "LocalCache.java" 3849]
    [com.google.common.cache.LocalCache getOrLoad "LocalCache.java" 3873]
    [com.google.common.cache.LocalCache$LocalLoadingCache get "LocalCache.java" 4798]
    [datomic.core.cache$fn__22003 invokeStatic "cache.clj" 71]
    [datomic.core.cache$fn__22003 invoke "cache.clj" 66]
    [datomic.core.cache$fn__21988$G__21983__21995 invoke "cache.clj" 63]
    [datomic.core.cache.WrappedGCache valAt "cache.clj" 120]
    [clojure.lang.RT get "RT.java" 760]
    [datomic.core.query$q_STAR_ invokeStatic "query.clj" 610]
    [datomic.core.query$q_STAR_ invoke "query.clj" 605]
    [datomic.core.local_query$local_q invokeStatic "local_query.clj" 58]
    [datomic.core.local_query$local_q invoke "local_query.clj" 52]
    [datomic.core.local_db$fn__25226 invokeStatic "local_db.clj" 28]
    [datomic.core.local_db$fn__25226 invoke "local_db.clj" 24]
    [datomic.client.api.impl$fn__11636$G__11629__11643 invoke "impl.clj" 33]
    [datomic.dev_local.impl$fn__16570$f__16571$fn__16572 invoke "impl.clj" 404]
    [datomic.dev_local.impl$fn__16570$f__16571 invoke "impl.clj" 404]
    [clojure.lang.AFn run "AFn.java" 22]
    [java.util.concurrent.ThreadPoolExecutor runWorker "ThreadPoolExecutor.java" 1149]
    [java.util.concurrent.ThreadPoolExecutor$Worker run "ThreadPoolExecutor.java" 624]
    [java.lang.Thread run "Thread.java" 748]]
  },
  :error :fault,
  :cognitect.anomalies/message "clojure.lang.ExceptionInfo: :db.error/unbound-query-variables Query is referencing unbound variables: #{?movie-titlex} {:cognitect.anomalies/category :cognitect.anomalies/incorrect, :cognitect.anomalies/message \"Query is referencing unbound variables: #{?movie-titlex}\", :variables #{?movie-titlex}, :db/error :db.error/unbound-query-variables}"
}
*/