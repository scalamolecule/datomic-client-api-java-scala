package datomicJava.client.api.sync;

import datomicClient.ErrorMsg;
import datomicJava.Setup;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static datomic.Util.*;
import static org.junit.Assert.assertThrows;

@FixMethodOrder(MethodSorters.JVM)
public class ClientTest extends Setup {

    public ClientTest(String name) {
        system = name;
    }

    @Test
    public void administerSystem() {
        if (system == "peer-server") {
            // administer-system not implemented for Peer Server
            RuntimeException nonExistingDb = assertThrows(
                RuntimeException.class,
                () -> client.administerSystem("hello")
            );
            assertThat(
                nonExistingDb.getMessage(),
                is(ErrorMsg.administerSystem())
            );

        } else {

            assertThat(
                client.administerSystem("hello"),
                is(anEmptyMap())
            );

            assertThat(
                client.administerSystem(
                    map(
                        read(":db-name"), "hello",
                        read(":action"), read(":upgrade-schema")
                    )
                ),
                is(anEmptyMap())
            );

            // todo - why doesn't this throw a failure exception with dev-local?
//            RuntimeException nonExistingDb = assertThrows(
//                RuntimeException.class,
//                () -> client.administerSystem("xyz")
//            );
//            assertThat(
//                nonExistingDb.getMessage(),
//                is("Some failure message...")
//            );
        }
    }

    @Test
    public void createDatabase() {
        if (isDevLocal()) {
            client.createDatabase("world");
            assertThat(client.listDatabases(), is(list("world", "hello")));

        } else {
            // create-database not implemented for Peer Server

            RuntimeException notImplemented = assertThrows(
                RuntimeException.class,
                () -> client.createDatabase("world")
            );
            assertThat(
                notImplemented.getMessage(),
                is(ErrorMsg.createDatabase("world"))
            );
        }
    }


    @Test
    public void deleteDatabase() {
        if (isDevLocal()) {
            client.deleteDatabase("hello");
            assertThat(client.listDatabases(), is(empty()));

        } else {
            // delete-database not implemented for Peer Server
            RuntimeException notImplemented = assertThrows(
                RuntimeException.class,
                () -> client.deleteDatabase("hello")
            );
            assertThat(
                notImplemented.getMessage(),
                is(ErrorMsg.deleteDatabase("hello"))
            );
        }
    }


    @Test
    public void listDatabases() {
        assertThat(client.listDatabases(), is(list("hello")));
    }
}
