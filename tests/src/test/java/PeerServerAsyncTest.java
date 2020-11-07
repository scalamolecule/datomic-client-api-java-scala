import clojure.java.api.Clojure;

import static datomic.Util.map;
import static datomic.Util.list;
import static datomic.Util.read;

public class PeerServerAsyncTest {

    /*
    Setup:
    1. Have a transactor running,
    2. Created a 'hello' database with Peer.createDatabase("hello")
    3. Started Peer Server (from pro distribution) with
    bin/run -m datomic.peer-server -h localhost -p 8998 -a myaccesskey,mysecret -d hello,datomic:free://localhost:4334/hello
    */

    public static void main(String[] args) {

        Clojure.var("clojure.core", "require").invoke(read("clojure.core.async"));
        Clojure.var("clojure.core", "require").invoke(read("cognitect.anomalies"));
        Clojure.var("clojure.core", "require").invoke(read("datomic.client.api.async"));

        Object client = Clojure.var("datomic.client.api.async", "client").invoke(
//            read(
//                "{:server-type :peer-server, " +
//                    ":access-key \"myaccesskey\", " +
//                    ":secret \"mysecret\", " +
//                    ":endpoint \"localhost:8998\", " +
//                    ":validate-hostnames false" +
//                    "}"
//            )

            // Can't use map - seems to be a bug in datomic
            map(
                read(":server-type"), read(":peer-server"),
                read(":access-key"), "myaccesskey",
                read(":secret"), "mysecret",
                read(":endpoint"), "localhost:8998",
                read(":validate-hostnames"), read("false")
            )
        );

        Object connCh = Clojure.var("datomic.client.api.async", "connect").invoke(
            client,
            read("{:db-name \"hello\"}")

            // Can't use map
//            map(read(":db-name"), "hello")
        );

        Object conn = Clojure.var("clojure.core.async", "<!!").invoke(connCh);

        Object db = Clojure.var("datomic.client.api.async", "db").invoke(conn);

        // Throws the clojure.lang.ExceptionInfo shown below
        Object ch = Clojure.var("datomic.client.api.async", "q").invoke(
            // Trying to represent db value as a String doesn't work
            read("{" +
                ":query [:find ?e :where [?e :db/ident :movie/title]], " +
                ":args [{" +
                ":t 2620, " +
                ":next-t 2624, " +
                ":db-name \"hello\", " +
                ":database-id \"datomic:dev://localhost:4334/hello\", " +
                ":type :datomic.client/db}]" +
                "}")

            // Can't use map either...
//            map(
//                read(":query"), read("[:find ?e :where [?e :db/ident :movie/title]]"),
//                read(":args"), list(db)
//            )
        );

        // Anomaly on the channel
        Object results = Clojure.var("clojure.core.async", "<!!").invoke(ch);
        System.out.println(results);
        // {:cognitect.anomalies/category :cognitect.anomalies/incorrect,
        //  :cognitect.anomalies/message "Query args must include a database"}

    }
}
