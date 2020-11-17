import datomicJava.SchemaAndData;
import datomicJava.client.api.sync.Client;
import datomicJava.client.api.sync.Connection;
import datomicJava.client.api.sync.Datomic;

import java.util.Collection;


public class PeerServerTest2 extends SchemaAndData {

    public static void main(String[] args) {
        Client client = Datomic.clientPeerServer("myaccesskey", "mysecret", "localhost:8998");
        Connection conn = client.connect("hello");
        conn.transact(schemaPeerServer);
        conn.transact(filmData);

        Collection<?> result = Datomic.q(
            "[:find ?e :where [?e :db/ident :movie/title]]",
            conn.db()
        );

        // [[72]]
        System.out.println(result);
    }
}
