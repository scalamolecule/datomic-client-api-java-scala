package issues;

import clojure.lang.Keyword;
import datomicClient.anomaly.CognitectAnomaly;
import datomicJava.SetupAsync;
import datomicJava.client.api.async.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static datomic.Util.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@FixMethodOrder(MethodSorters.JVM)
public class issue1 extends SetupAsync {

    public issue1(String name) {
        system = name;
    }

    @Test
    public void issue1() throws ExecutionException, InterruptedException {
        AsyncClient sclient = AsyncDatomic.clientPeerServer("k", "s", "localhost:8998");
        CompletableFuture<Either<CognitectAnomaly, AsyncConnection>> fcon = sclient.connect("hello");
        Either<CognitectAnomaly, AsyncConnection> either = fcon.get();
        Right right = (Right) either;
        AsyncConnection con = (AsyncConnection) right.right_value();
        AsyncDb sdb = con.db();
        CompletableFuture<Channel<Stream<?>>> qr = AsyncDatomic.q(
            map(
                Keyword.intern("query"), read("[:find ?movie-title :where [_ :movie/title ?movie-title]]"),
                Keyword.intern("args"), list(sdb.datomicDb())
            )
        );

        // chunk() works now...
        assertThat(
            //
            ((Right<?, Stream<?>>) qr.get().chunk()).right_value().toArray(),
            is(list(list("Commando"), list("The Goonies"), list("Repo Man")).toArray())
        );
    }
}
