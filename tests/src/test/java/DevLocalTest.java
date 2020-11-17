import datomicJava.SchemaAndData;
import datomicJava.client.api.sync.Client;
import datomicJava.client.api.sync.Connection;
import datomicJava.client.api.sync.Datomic;

import java.util.Collection;

import static datomic.Util.*;

public class DevLocalTest extends SchemaAndData {


    public static void main(String[] args) {
        Client client = Datomic.clientDevLocal("dev");
        client.deleteDatabase("hello");
        client.deleteDatabase("world");
        client.createDatabase("hello");
        Connection conn = client.connect("hello");
        conn.transact(schemaDevLocal);
        conn.transact(filmData);

        Collection<?> result = Datomic.q(
            map(
                read(":query"), read("[:find ?e :where [?e :db/ident :movie/title]]"),
                read(":args"), list(conn.db().datomicDb())
            )
        );

        assert (result.toString().equals("[[73]]"));
        System.out.println(result);
    }
}
