import clojure.java.api.Clojure;
import clojure.lang.PersistentVector;

import static datomic.Util.list;
import static datomic.Util.map;
import static datomic.Util.read;

public class PeerServerTest {

    /*
    Setup:
    1. Have a transactor running,
    2. Created a 'hello' database with Peer.createDatabase("hello")
    3. Started Peer Server (from pro distribution) with
    bin/run -m datomic.peer-server -h localhost -p 8998 -a myaccesskey,mysecret -d hello,datomic:dev://localhost:4334/hello
    */

    public static void main(String[] args) {

        Clojure.var("clojure.core", "require").invoke(read("datomic.client.api"));

        Object client = Clojure.var("datomic.client.api", "client").invoke(
//            read(
//                "{" +
//                    ":server-type :peer-server, " +
//                    ":access-key \"myaccesskey\", " +
//                    ":secret \"mysecret\", " +
//                    ":endpoint \"localhost:8998\", " +
//                    ":validate-hostnames false" +
//                    "}"
//            )

            // Can't use map
            map(
                read(":server-type"), read(":peer-server"),
                read(":access-key"), "myaccesskey",
                read(":secret"), "mysecret",
                read(":endpoint"), "localhost:8998",
                read(":validate-hostnames"), read("false")
            )
        );

        Object conn = Clojure.var("datomic.client.api", "connect").invoke(
            client,
            read("{:db-name \"hello\"}")

            // Can't use map
//            map(read(":db-name"), "hello")
        );

        Object db = Clojure.var("datomic.client.api", "db").invoke(conn);

        PersistentVector results =
            (PersistentVector) Clojure.var("datomic.client.api", "q").invoke(
                // Why is map allowed here but not above / in async?
                map(
                    read(":query"), read("[:find ?e :where [?e :db/ident :movie/title]]"),
                    read(":args"), list(db)
                )
            );

        // Attribute :movie/title exists
        assert (results.size() == 1);


        // Throws
        // WARNING: requiring-resolve already refers to: #'clojure.core/requiring-resolve in namespace: datomic.common,
        // being replaced by: #'datomic.common/requiring-resolve

        // Can I change my import to avoid this collision using :refer or something?

        Object results2 = Clojure.var("datomic.client.api", "qseq").invoke(
            map(
                read(":query"), read("[:find ?e :where [?e :db/ident :movie/title]]"),
                read(":args"), list(db)
            )
        );

        System.out.println(results2);
    }
}
