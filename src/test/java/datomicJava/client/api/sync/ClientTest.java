package datomicJava.client.api.sync;

import datomicClojure.ErrorMsg;
import datomicJava.Setup;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
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
        // todo: Not available for Peer Server?
        assertThat(
            client.administerSystem(
                "{:db-name \"hello\" :action :upgrade-schema}"
            ),
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
            assertThat(client.listDatabases(), is(list("hello")));
            client.createDatabase("world");
            assertThat(client.listDatabases(), is(list("world", "hello")));

            // Delete db
            client.deleteDatabase("world");
            assertThat(client.listDatabases(), is(list("hello")));

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
