package datomicJava.client.api.async;

import datomicClojure.ErrorMsg;
import datomicJava.SetupAsync;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static datomic.Util.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThrows;

@FixMethodOrder(MethodSorters.JVM)
public class AsyncClientTest extends SetupAsync {

    public AsyncClientTest(String name) {
        system = name;
    }

    @Test
    public void administerSystem() {
        // todo: Not available for Peer Server?
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

        // todo - why doesn't this throw a failure exception?
        RuntimeException nonExistingDb = assertThrows(
            RuntimeException.class,
            () -> client.administerSystem("xyz")
        );
        assertThat(
            nonExistingDb.getMessage(),
            is("Some failure message...")
        );
    }

    @Test
    public void createDatabase() {
        if (isDevLocal()) {
            client.createDatabase("world");
            assertThat(
                client.listDatabases().realize(),
                is(list("world", "hello"))
            );

        } else {
            // create-database not implemented for Peer Server

            RuntimeException notImplemented = assertThrows(
                RuntimeException.class,
                () -> client.createDatabase("world").realize()
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
            client.deleteDatabase("hello").realize();
            assertThat(client.listDatabases().realize(), is(empty()));

        } else {
            // delete-database not implemented for Peer Server
            RuntimeException notImplemented = assertThrows(
                RuntimeException.class,
                () -> client.deleteDatabase("hello").realize()
            );
            assertThat(
                notImplemented.getMessage(),
                is(ErrorMsg.deleteDatabase("hello"))
            );
        }
    }


    @Test
    public void listDatabases() {
        assertThat(
            client.listDatabases().realize(),
            is(list("hello"))
        );
    }
}
