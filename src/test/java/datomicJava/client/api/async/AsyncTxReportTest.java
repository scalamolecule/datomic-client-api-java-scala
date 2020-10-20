package datomicJava.client.api.async;

import datomicJava.SetupAsync;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsEmptyCollection.empty;


public class AsyncTxReportTest extends SetupAsync {

    public AsyncTxReportTest(String name) {
        system = name;
    }

    @Test
    public void txReportOps() throws ExecutionException, InterruptedException {
        assertThat(films(txReport.dbBefore()), is(empty()));
        assertThat(films(txReport.dbAfter()), is(threeFilms));

        // Tx datom + 3 entities * 3 attributes transacted
        assertThat(txReport.txData().count(), is((long) 1 + 3 * 3));

        if (isDevLocal()) {
            // No temp ids created with dev-local setup
            assertThat(txReport.tempIds().size(), is(0));
        } else {
            assertThat(txReport.tempIds().size(), is(3));
        }
    }
}