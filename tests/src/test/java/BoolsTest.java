import datomic.Connection;
import datomic.Peer;
import datomicJava.SchemaAndData;
import datomicJava.client.api.sync.Client;
import datomicJava.client.api.sync.Datomic;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static datomic.Util.*;

public class BoolsTest extends SchemaAndData {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        peerTest();
        clientTest();
    }

    public static void peerTest() throws ExecutionException, InterruptedException {
        Peer.createDatabase("datomic:mem://test1");
        Connection conn = Peer.connect("datomic:mem://test1");
        conn.transact(
            list(
                map(
                    read(":db/ident"), read(":ns/int"),
                    read(":db/valueType"), read(":db.type/long"),
                    read(":db/cardinality"), read(":db.cardinality/one")
                ),
                map(
                    read(":db/ident"), read(":ns/bools"),
                    read(":db/valueType"), read(":db.type/boolean"),
                    read(":db/cardinality"), read(":db.cardinality/many")
                )
            )
        ).get();

        conn.transact(
            list(
                map(
                    read(":ns/int"), 1,
                    read(":ns/bools"), list(true, false)
                )
            )
        ).get();

        Collection<List<Object>> res = Peer.q(
            "[:find ?e ?int (distinct ?bools) " +
                ":where [?e :ns/int ?int][?e :ns/bools ?bools]]",
            conn.db()
        );
        Long eid = (Long) res.iterator().next().get(0);

        System.out.println(res);
        // [[17592186045418 1 #{true false}]]

        System.out.println(conn.db().pull("[*]", eid));
        // Getting only true ...
        // {:db/id 17592186045418, :ns/int 1, :ns/bools [true]}
        // Expected Set of true and false
        // {:db/id 17592186045418, :ns/int 1, :ns/bools [false, true]}
    }

    public static void clientTest() {
        // Connecting to client via clojure client api facade
        Client client = datomicJava.client.api.sync.Datomic.clientDevLocal("mem");
        client.createDatabase("test2");
        datomicJava.client.api.sync.Connection conn = client.connect("test2");

        conn.transact(
            list(
                map(
                    read(":db/ident"), read(":ns/int"),
                    read(":db/valueType"), read(":db.type/long"),
                    read(":db/cardinality"), read(":db.cardinality/one")
                ),
                map(
                    read(":db/ident"), read(":ns/bools"),
                    read(":db/valueType"), read(":db.type/boolean"),
                    read(":db/cardinality"), read(":db.cardinality/many")
                )
            )
        );

        conn.transact(
            list(
                map(
                    read(":ns/int"), 1,
                    read(":ns/bools"), list(true, false)
                )
            )
        );


        Collection<List<Object>> res = Datomic.q(
            "[:find ?e ?int (distinct ?bools) " +
                ":where [?e :ns/int ?int][?e :ns/bools ?bools]]",
            conn.db()
        );
        Long eid = (Long) res.iterator().next().get(0);

        System.out.println(res);
        // [[74766790688843 1 #{true false}]]

        System.out.println(conn.db().pull("[*]", eid));
        // Getting expected result
        // {:db/id 74766790688843, :ns/int 1, :ns/bools [false true]}
    }
}
