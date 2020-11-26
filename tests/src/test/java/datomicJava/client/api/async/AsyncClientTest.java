package datomicJava.client.api.async;

import datomicClient.ErrorMsg;
import datomicJava.SetupAsync;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.List;
import java.util.concurrent.ExecutionException;

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
            RuntimeException nonExistingDb = assertThrows(
                RuntimeException.class,
                () -> client.administerSystem("xyz")
            );
            assertThat(
                nonExistingDb.getMessage(),
                is("Some failure message...")
            );
        }
    }

    @Test
    public void createDatabase() throws ExecutionException, InterruptedException {
        if (isDevLocal()) {
            client.createDatabase("world").get();
            assertThat(
                ((Right<?, List<String>>) client.listDatabases().get()).right_value(),
                is(list("world", "hello"))
            );

        } else {
            // create-database not implemented for Peer Server

            RuntimeException notImplemented = assertThrows(
                RuntimeException.class,
                () -> client.createDatabase("world").get()
            );
            assertThat(
                notImplemented.getMessage(),
                is(ErrorMsg.createDatabase("world"))
            );
        }
    }


    @Test
    public void deleteDatabase() throws ExecutionException, InterruptedException {
        if (isDevLocal()) {
            client.deleteDatabase("hello").get();
            assertThat(
                ((Right<?, List<String>>) client.listDatabases().get()).right_value(),
                is(empty())
            );

        } else {
            // delete-database not implemented for Peer Server
            RuntimeException notImplemented = assertThrows(
                RuntimeException.class,
                () -> client.deleteDatabase("hello").get()
            );
            assertThat(
                notImplemented.getMessage(),
                is(ErrorMsg.deleteDatabase("hello"))
            );
        }
    }


    @Test
    public void listDatabases() throws ExecutionException, InterruptedException {
        assertThat(
            ((Right<?, List<String>>) client.listDatabases().get()).right_value(),
            is(list("hello"))
        );
    }
}
