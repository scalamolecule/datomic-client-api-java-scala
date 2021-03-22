package datomicJava.client.api.sync;

import datomicClient.ErrorMsg;
import datomicClient.anomaly.Interrupted;
import datomicJava.Setup;
import datomicJava.client.api.Datom;
import datomicJava.client.api.DbStats;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

import static datomic.Util.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThrows;

@FixMethodOrder(MethodSorters.JVM)
public class DbTest extends Setup {

    public DbTest(String name) {
        system = name;
    }

    @Test
    public void stats() {
        Db db = conn.db();
        assertThat(db.dbName(), is("hello"));
        assertThat(db.t(), is(tAfter()));
        assertThat(db.asOfT(), is(0L));
        assertThat(db.sinceT(), is(0L));
        assertThat(db.isHistory(), is(false));

        if (isDevLocal()) {
            DbStats dbStats = new DbStats(
                243,
                map(
                    ":db.install/partition", 3L,
                    ":db.install/valueType", 15L,
                    ":db.install/attribute", 30L,
                    ":db/ident", 58L,
                    ":db/valueType", 30L,
                    ":db/tupleType", 2L,
                    ":db/cardinality", 30L,
                    ":db/unique", 1L,
                    ":db/doc", 42L,
                    ":db/txInstant", 7L,
                    ":db/fulltext", 1L,
                    ":fressian/tag", 15L,
                    ":movie/genre", 3L,
                    ":movie/release-year", 3L,
                    ":movie/title", 3L
                )
            );

            assertThat(db.dbStats().datoms(), is(dbStats.datoms()));
            assertThat(
                db.dbStats().attrs().get(":db.install/partition"),
                is(dbStats.attrs().get(":db.install/partition"))
            );

        } else {
            // Peer server db is not re-created on each test,
            // so we can only test some stable values
            assertThat(db.dbStats().datoms(), is(greaterThan(0L)));
            assertThat(db.dbStats().attrs().get(":db.install/partition"), is(3L));
        }
    }


    @Test
    public void lookupAsOf() throws InterruptedException {
        // Make sure there's at leas 1 ms between dates
        Thread.sleep(5);
        TxReport txReport4 = conn.transact(film4);
        Thread.sleep(5);
        TxReport txReport5 = conn.transact(film5);
        Db db = conn.db();

        Long t4basis = txReport4.basisT();
        Long t4 = txReport4.t();
        Long tx4 = txReport4.tx();
        Date txInst4 = txReport4.txInst();

        Long t5basis = txReport5.basisT();
        Long t5 = txReport5.t();
        Long tx5 = txReport5.tx();
        Date txInst5 = txReport5.txInst();

        // db name and history unaffected by as-of filter
        assertThat(db.dbName(), is("hello"));
        assertThat(db.asOf(t4).dbName(), is("hello"));

        assertThat(db.isHistory(), is(false));
        assertThat(db.asOf(t4).isHistory(), is(false));

        // basis-t is the t of the most recent transaction
        assertThat(t4basis, is(tAfter()));
        assertThat(t5basis, is(t4));

        // Current t is that of transaction 5
        assertThat(db.t(), is(t5));

        // t of as-of db is still the same as the un-filtered db
        assertThat(db.asOf(t4).t(), is(db.t()));
        assertThat(db.asOf(tx4).t(), is(db.t()));
        assertThat(db.asOf(txInst4).t(), is(db.t()));

        assertThat(db.asOf(t5).t(), is(db.t()));
        assertThat(db.asOf(tx5).t(), is(db.t()));
        assertThat(db.asOf(txInst5).t(), is(db.t()));

        // Use asOfT to retrieve as-of t of filtered db
        assertThat(db.asOf(t4).asOfT(), is(t4));
        assertThat(db.asOf(tx4).asOfT(), is(t4));
        assertThat(db.asOf(txInst4).asOfT(), is(t4));

        assertThat(db.asOf(t5).asOfT(), is(t5));
        assertThat(db.asOf(tx5).asOfT(), is(t5));
        assertThat(db.asOf(txInst5).asOfT(), is(t5));

        // Un-filtered db has no as-of t
        assertThat(db.asOfT(), is(0L));

        // Use asOfTxInst to retrieve as-of tx instant of filtered db
        assertThat(db.asOf(t4).asOfTxInst(), is(txInst4));
        assertThat(db.asOf(tx4).asOfTxInst(), is(txInst4));
        assertThat(db.asOf(txInst4).asOfTxInst(), is(txInst4));

        assertThat(db.asOf(t5).asOfTxInst(), is(txInst5));
        assertThat(db.asOf(tx5).asOfTxInst(), is(txInst5));
        assertThat(db.asOf(txInst5).asOfTxInst(), is(txInst5));


        // as-of-filtered db has no sinceT
        assertThat(db.asOf(t4).sinceT(), is(0L));
        assertThat(db.asOf(tx4).sinceT(), is(0L));
        assertThat(db.asOf(txInst4).sinceT(), is(0L));
        assertThat(db.asOf(t5).sinceT(), is(0L));
        assertThat(db.asOf(tx5).sinceT(), is(0L));
        assertThat(db.asOf(txInst5).sinceT(), is(0L));

        // as-of-filtered db has no sinceTxInst
        assertThat(db.asOf(t4).sinceTxInst(), is(nullValue()));
        assertThat(db.asOf(tx4).sinceTxInst(), is(nullValue()));
        assertThat(db.asOf(txInst4).sinceTxInst(), is(nullValue()));
        assertThat(db.asOf(t5).sinceTxInst(), is(nullValue()));
        assertThat(db.asOf(tx5).sinceTxInst(), is(nullValue()));
        assertThat(db.asOf(txInst5).sinceTxInst(), is(nullValue()));
    }


    @Test
    public void lookupSince() throws InterruptedException {
        // Make sure there's at leas 1 ms between dates
        Thread.sleep(5);
        TxReport txReport4 = conn.transact(film4);
        Thread.sleep(5);
        TxReport txReport5 = conn.transact(film5);
        Db db = conn.db();

        Long t4basis = txReport4.basisT();
        Long t4 = txReport4.t();
        Long tx4 = txReport4.tx();
        Date txInst4 = txReport4.txInst();

        Long t5basis = txReport5.basisT();
        Long t5 = txReport5.t();
        Long tx5 = txReport5.tx();
        Date txInst5 = txReport5.txInst();

        // db name and history unaffected by as-of filter
        assertThat(db.dbName(), is("hello"));
        assertThat(db.since(t4).dbName(), is("hello"));

        assertThat(db.isHistory(), is(false));
        assertThat(db.since(t4).isHistory(), is(false));

        // basis-t is the t of the most recent transaction
        assertThat(t4basis, is(tAfter()));
        assertThat(t5basis, is(t4));

        // Current t is that of transaction 5
        assertThat(db.t(), is(t5));

        // t of since db is still the same as the un-filtered db
        assertThat(db.since(t4).t(), is(db.t()));
        assertThat(db.since(tx4).t(), is(db.t()));
        assertThat(db.since(txInst4).t(), is(db.t()));

        assertThat(db.since(t5).t(), is(db.t()));
        assertThat(db.since(tx5).t(), is(db.t()));
        assertThat(db.since(txInst5).t(), is(db.t()));

        // Use sinceT to retrieve since t of filtered db
        assertThat(db.since(t4).sinceT(), is(t4));
        assertThat(db.since(tx4).sinceT(), is(t4));
        assertThat(db.since(txInst4).sinceT(), is(t4));

        assertThat(db.since(t5).sinceT(), is(t5));
        assertThat(db.since(tx5).sinceT(), is(t5));
        assertThat(db.since(txInst5).sinceT(), is(t5));

        // Un-filtered db has no since t
        assertThat(db.sinceT(), is(0L));

        // Use sinceTxInst to retrieve since tx instant of filtered db.
        // Normally this is not available since the since-filtered db excludes
        // the time point, but we are cache it when applying the filter on the
        // still unfiltered db.
        assertThat(db.since(t4).sinceTxInst(), is(txInst4));
        assertThat(db.since(tx4).sinceTxInst(), is(txInst4));
        assertThat(db.since(txInst4).sinceTxInst(), is(txInst4));

        assertThat(db.since(t5).sinceTxInst(), is(txInst5));
        assertThat(db.since(tx5).sinceTxInst(), is(txInst5));
        assertThat(db.since(txInst5).sinceTxInst(), is(txInst5));


        // since-filtered db has no asOfT
        assertThat(db.since(t4).asOfT(), is(0L));
        assertThat(db.since(tx4).asOfT(), is(0L));
        assertThat(db.since(txInst4).asOfT(), is(0L));
        assertThat(db.since(t5).asOfT(), is(0L));
        assertThat(db.since(tx5).asOfT(), is(0L));
        assertThat(db.since(txInst5).asOfT(), is(0L));

        // since-filtered db has no asOfTxInst
        assertThat(db.since(t4).asOfTxInst(), is(nullValue()));
        assertThat(db.since(tx4).asOfTxInst(), is(nullValue()));
        assertThat(db.since(txInst4).asOfTxInst(), is(nullValue()));
        assertThat(db.since(t5).asOfTxInst(), is(nullValue()));
        assertThat(db.since(tx5).asOfTxInst(), is(nullValue()));
        assertThat(db.since(txInst5).asOfTxInst(), is(nullValue()));
    }


    @Test
    public void lookupHistory() {
        Db db = conn.db();

        assertThat(db.history().dbName(), is("hello"));
        assertThat(db.history().t(), is(tAfter()));
        assertThat(db.history().asOfT(), is(0L));
        assertThat(db.history().sinceT(), is(0L));
        assertThat(db.history().isHistory(), is(true));
    }


    @Test
    public void asOf() {

        // Current state
        assertThat(films(conn.db()), is(threeFilms));

        // State before last tx
        assertThat(films(conn.db().asOf(tBefore())), is(empty()));
        assertThat(films(conn.db().asOf(txBefore)), is(empty()));
        assertThat(films(conn.db().asOf(txInstBefore)), is(empty()));

        // State after last tx same as current state
        assertThat(films(conn.db().asOf(tAfter())), is(threeFilms));
        assertThat(films(conn.db().asOf(txAfter())), is(threeFilms));
        assertThat(films(conn.db().asOf(txInstAfter())), is(threeFilms));

        // We can use the transaction id too
        assertThat(films(conn.db().asOf(txBefore)), is(empty()));
    }


    @Test
    public void since() {
        // State created since previous t
        assertThat(films(conn.db().since(tBefore())), is(threeFilms));
        assertThat(films(conn.db().since(txBefore)), is(threeFilms));
        assertThat(films(conn.db().since(txInstBefore)), is(threeFilms));

        // Nothing created after
        assertThat(films(conn.db().since(tAfter())), is(empty()));
        assertThat(films(conn.db().since(txAfter())), is(empty()));
        assertThat(films(conn.db().since(txInstAfter())), is(empty()));
    }


    @Test
    public void withJavaStmts() {
        // Original state
        assertThat(films(conn.db()), is(threeFilms));

        // Test adding a 4th film
        // OBS: Note that a `conn.withDb` has to be passed initially!
        TxReport txReport4films = conn.db().with(conn.withDb(), film4);
        Db db4Films = txReport4films.dbAfter();
        assertThat(films(db4Films), is(fourFilms));

        // Test adding a 4th film
        // OBS: Note that a `conn.withDb` has to be passed initially!
        TxReport txReport5films = conn.db().with(db4Films, film5);
        Db db5Films = txReport5films.dbAfter();
        assertThat(films(db5Films), is(fiveFilms));

        // Test adding a 4th film
        // OBS: Note that a `conn.withDb` has to be passed initially!
        TxReport txReport6films = conn.db().with(txReport5films, film6);
        Db db6Films = txReport6films.dbAfter();
        assertThat(films(db6Films), is(sixFilms));

        // Combining `with` and `asOf`
        // todo: peer-server doesn't allow combining `with` filter with other filters
        if (system == "dev-local")
            assertThat(films(db6Films.asOf(txReport5films.tx())), is(fiveFilms));

        // Original state is unaffected
        assertThat(films(conn.db()), is(threeFilms));
    }

    @Test
    public void withEdnFile() throws FileNotFoundException {
        // Original state
        assertThat(films(conn.db()), is(threeFilms));

        // Test adding a 4th film
        // OBS: Note that a `conn.withDb` has to be passed initially!
        TxReport txReport4films = conn.db().with(
            conn.withDb(), new FileReader("tests/resources/film4.edn")
        );
        Db db4Films = txReport4films.dbAfter();
        assertThat(films(db4Films), is(fourFilms));

        // Test adding a 4th film
        // OBS: Note that a `conn.withDb` has to be passed initially!
        TxReport txReport5films = conn.db().with(
            db4Films, new FileReader("tests/resources/film5.edn")
        );
        Db db5Films = txReport5films.dbAfter();
        assertThat(films(db5Films), is(fiveFilms));

        // Test adding a 4th film
        // OBS: Note that a `conn.withDb` has to be passed initially!
        TxReport txReport6films = conn.db().with(
            txReport5films, new FileReader("tests/resources/film6.edn")
        );
        Db db6Films = txReport6films.dbAfter();
        assertThat(films(db6Films), is(sixFilms));

        // Combining `with` and `asOf`
        // todo: peer-server doesn't allow combining `with` filter with other filters
        if (system == "dev-local")
            assertThat(films(db6Films.asOf(txReport5films.tx())), is(fiveFilms));

        // Original state is unaffected
        assertThat(films(conn.db()), is(threeFilms));
    }

    @Test
    public void withEdnString() {
        // Original state
        assertThat(films(conn.db()), is(threeFilms));

        // Test adding a 4th film
        // OBS: Note that a `conn.withDb` has to be passed initially!
        TxReport txReport4films = conn.db().with(
            conn.withDb(), "[ {:movie/title \"Film 4\"} ]"
        );
        Db db4Films = txReport4films.dbAfter();
        assertThat(films(db4Films), is(fourFilms));

        // Test adding a 4th film
        // OBS: Note that a `conn.withDb` has to be passed initially!
        TxReport txReport5films = conn.db().with(
            db4Films, "[ {:movie/title \"Film 5\"} ]"
        );
        Db db5Films = txReport5films.dbAfter();
        assertThat(films(db5Films), is(fiveFilms));

        // Test adding a 4th film
        // OBS: Note that a `conn.withDb` has to be passed initially!
        TxReport txReport6films = conn.db().with(
            txReport5films, "[ {:movie/title \"Film 6\"} ]"
        );
        Db db6Films = txReport6films.dbAfter();
        assertThat(films(db6Films), is(sixFilms));

        // Combining `with` and `asOf`
        // todo: peer-server doesn't allow combining `with` filter with other filters
        if (system == "dev-local")
            assertThat(films(db6Films.asOf(txReport5films.tx())), is(fiveFilms));

        // Original state is unaffected
        assertThat(films(conn.db()), is(threeFilms));
    }


    @Test
    public void withJavaStmts_SingleInvocation() {
        // As a convenience, a single-invocation shorter version of `with`:
        assertThat(films(conn.widh(film4)), is(fourFilms));

        // Applying another data set still augments the original db
        assertThat(films(conn.widh(film4and5)), is(fiveFilms));


        // Current state is unaffected
        assertThat(films(conn.db()), is(threeFilms));
    }

    @Test
    public void withEdnFile_SingleInvocation() throws FileNotFoundException {
        // As a convenience, a single-invocation shorter version of `with`:
        assertThat(
            films(conn.widh(new FileReader("tests/resources/film4.edn"))),
            is(fourFilms)
        );

        // Applying another data set still augments the original db
        assertThat(
            films(conn.widh(new FileReader("tests/resources/film4and5.edn"))),
            is(fiveFilms)
        );

        // Current state is unaffected
        assertThat(films(conn.db()), is(threeFilms));
    }

    @Test
    public void withEdnString_SingleInvocation() {
        // As a convenience, a single-invocation shorter version of `with`:
        assertThat(
            films(conn.widh("[ {:movie/title \"Film 4\"} ]")),
            is(fourFilms)
        );

        // Applying another data set still augments the original db
        assertThat(
            films(conn.widh("[ {:movie/title \"Film 4\"} {:movie/title \"Film 5\"} ]")),
            is(fiveFilms)
        );

        // Current state is unaffected
        assertThat(films(conn.db()), is(threeFilms));
    }

    @Test
    public void history() {

        // Not testing Peer Server history since history is accumulating when we
        // can't re-create database for each test without shutting down Peer Server.

        if (isDevLocal()) {

            // Current and history db are currently the same
            assertThat(films(conn.db()), is(threeFilms));
            assertThat(films(conn.db().history()), is(threeFilms));

            // As long as we only add data, current/history will be the same
            TxReport tx = conn.transact(film4);
            assertThat(films(conn.db()), is(fourFilms));
            assertThat(films(conn.db().history()), is(fourFilms));

            // Now retract the last entity
            Iterator<Datom> it = tx.txData().iterator();
            long lastEid = 0L;
            while (it.hasNext()) lastEid = it.next().e();
            conn.transact(list(list(read(":db/retractEntity"), lastEid)));

            assertThat(films(conn.db()), is(threeFilms));
            assertThat(films(conn.db().history()), is(fourFilms));

            // History of movie title assertions and retractions
            Collection<List<Object>> history = Datomic.q(
                "[:find ?tx ?added ?movie-title :where [_ :movie/title ?movie-title ?tx ?added]]",
                conn.db().history() // Use history database
            );
            List<String> historyStr = new ArrayList<>();
            history.forEach(row -> historyStr.add(row.toString()));
            Collections.sort(historyStr);
            assertThat(
                historyStr,
                is(list(
                    // First tx
                    "[13194139533319 true \"Commando\"]",
                    "[13194139533319 true \"Repo Man\"]",
                    "[13194139533319 true \"The Goonies\"]",

                    // Film 4 added
                    "[13194139533320 true \"Film 4\"]",

                    // Film 4 retracted
                    "[13194139533321 false \"Film 4\"]"
                ))
            );
        }
    }

    private List<Datom> datoms(
        String index,
        List<?> components,
        Integer timeout,
        Integer offset,
        Integer limit
    ) {
        Iterator<Datom> it = conn.db()
            .datoms(index, components, timeout, offset, limit).iterator();
        List<Datom> datoms = new ArrayList<>();
        while (it.hasNext()) {
            datoms.add(it.next());
        }
        return datoms;
    }

    private List<Datom> datoms(
        String index,
        List<?> components
    ) {
        return datoms(index, components, 0, 0, 1000);
    }

    @Test
    public void datomsAVET() {

        // AVET index is sorted by
        // Attribute (id, not name!) - Value - Entity id - Transaction id

        // Supply A value (as clojure.lang.Keyword)
        // Get all datoms of attribute :movie/title
        assertThat(datoms(
            ":avet",
            list(read(":movie/title"))
        ), is(list(
            new Datom(e2(), a1(), "Commando", txAfter(), true),
            new Datom(e3(), a1(), "Repo Man", txAfter(), true),
            new Datom(e1(), a1(), "The Goonies", txAfter(), true)
        )));

        // For brevity we only show the value in following tests...

        // A and V
        assertThat(datoms(
            ":avet",
            list(read(":movie/title"), "Commando")
        ), is(list(
            new Datom(e2(), a1(), "Commando", txAfter(), true)
        )));

        // A, V and E (e2 is the eid of the Commando film entity)
        assertThat(datoms(
            ":avet",
            list(read(":movie/title"), "Commando", e2())
        ), is(list(
            new Datom(e2(), a1(), "Commando", txAfter(), true)
        )));

        // A, V, E and T (tAfter is the time point of the film saving tx)
        // Time point T can be a t or tx (not a txInstant / Date)
        assertThat(datoms(
            ":avet",
            list(read(":movie/title"), "Commando", e2(), tAfter())
        ), is(list(
            new Datom(e2(), a1(), "Commando", txAfter(), true)
        )));

        // (only dev-local is re-created on each test and therefore has a stable size)
        if (system == "dev-local") {
            // We can supply an empty components list and get the entire (!) db (requires
            // though to set limit = -1)
            assertThat(datoms(
                ":avet",
                list(),
                0, 0, -1 // to fetch all!
            ).size(), is(243));
        }

        // limit number of datoms returned
        assertThat(datoms(
            ":avet",
            list(read(":movie/title")),
            0, 0, 2
        ), is(list(
            new Datom(e2(), a1(), "Commando", txAfter(), true),
            new Datom(e3(), a1(), "Repo Man", txAfter(), true)
        )));

        // Add offset for first datom in index to return
        assertThat(datoms(
            ":avet",
            list(read(":movie/title")),
            0, 1, 2
        ), is(list(
            new Datom(e3(), a1(), "Repo Man", txAfter(), true),
            new Datom(e1(), a1(), "The Goonies", txAfter(), true)
        )));
    }


    @Test
    public void datomsEAVT() {

        // EAVT index is sorted by
        // Entity id - Attribute id (not name!) - Value - Transaction id

        // E - Get all datoms of entity e2
        assertThat(datoms(
            ":eavt",
            list(e2())
        ), is(list(
            new Datom(e2(), a1(), "Commando", txAfter(), true),
            new Datom(e2(), a2(), "thriller/action", txAfter(), true),
            new Datom(e2(), a3(), 1985, txAfter(), true)
        )));

        // EA
        assertThat(datoms(
            ":eavt",
            list(e2(), read(":movie/title"))
        ), is(list(
            new Datom(e2(), a1(), "Commando", txAfter(), true)
        )));

        // EAV
        assertThat(datoms(
            ":eavt",
            list(e2(), read(":movie/title"), "Commando")
        ), is(list(
            new Datom(e2(), a1(), "Commando", txAfter(), true)
        )));

        // EAVT
        assertThat(datoms(
            ":eavt",
            list(e2(), read(":movie/title"), "Commando", txAfter())
        ), is(list(
            new Datom(e2(), a1(), "Commando", txAfter(), true)
        )));
    }


    @Test
    public void datomsAEVT() {

        // AEVT index is sorted by
        // Attribute id (not name!) - Entity id - Value - Transaction id

        // A
        assertThat(datoms(
            ":aevt",
            list(read(":movie/title"))
        ), is(list(
            new Datom(e1(), a1(), "The Goonies", txAfter(), true),
            new Datom(e2(), a1(), "Commando", txAfter(), true),
            new Datom(e3(), a1(), "Repo Man", txAfter(), true)
        )));

        // AE
        assertThat(datoms(
            ":aevt",
            list(read(":movie/title"), e2())
        ), is(list(
            new Datom(e2(), a1(), "Commando", txAfter(), true)
        )));

        // AEV
        assertThat(datoms(
            ":aevt",
            list(read(":movie/title"), e2(), "Commando")
        ), is(list(
            new Datom(e2(), a1(), "Commando", txAfter(), true)
        )));

        // AEVT
        assertThat(datoms(
            ":aevt",
            list(read(":movie/title"), e2(), "Commando", txAfter())
        ), is(list(
            new Datom(e2(), a1(), "Commando", txAfter(), true)
        )));
    }


    @Test
    public void datomsVAET() {
        // The VAET index is for following relationships in reverse.
        // No ref type is defined here, but it follows the same pattern as
        // shown above.
        // See https://docs.datomic.com/cloud/query/raw-index-access.html#vaet

        // VAET index is sorted by
        // Value (ref value) - Attribute id (not name!) - Entity id - Transaction id
    }


    private List<String> indexRange(
        String attrId,
        Object start,
        Object end,
        Integer timeout,
        Integer offset,
        Integer limit
    ) {
        List<String> titles = new ArrayList<>();
        Iterator<Datom> datoms = conn.db().indexRange(
            attrId, start, end, timeout, offset, limit
        ).iterator();
        while (datoms.hasNext()) {
            titles.add(
                datoms.next().v().toString()
            );
        }
        return titles;
    }

    private List<String> indexRange(String attrId, Object start, Object end) {
        return indexRange(attrId, start, end, 0, 0, 1000);
    }

    private List<String> indexRange(String attrId, Object start) {
        return indexRange(attrId, start, null, 0, 0, 1000);
    }

    private List<String> indexRange(String attrId) {
        return indexRange(attrId, null, null, 0, 0, 1000);
    }

    @Test
    public void indexRange() {

        // Datoms from AVET index sorted by attribute-value-entity-tx

        // This retrieves the first 1000 datoms for :movie/title
        Iterator<Datom> datoms = conn.db().indexRange(":movie/title").iterator();

        List<Datom> datomsCheck = new ArrayList<>();
        datomsCheck.add(new Datom(e2(), a1(), "Commando", txAfter(), true));
        datomsCheck.add(new Datom(e3(), a1(), "Repo Man", txAfter(), true));
        datomsCheck.add(new Datom(e1(), a1(), "The Goonies", txAfter(), true));
        Iterator<Datom> datomsCheckIt = datomsCheck.iterator();

        while (datoms.hasNext()) {
            assertThat(datoms.next(), is(datomsCheckIt.next()));
        }

        // The test above in short format:
        assertThat(
            indexRange(":movie/title"),
            is(list("Commando", "Repo Man", "The Goonies"))
        );

        // Retrieve all (!) datoms for :movie/title (in this case just 3)
        assertThat(
            indexRange(":movie/title", null, null, 0, 0, -1),
            is(list("Commando", "Repo Man", "The Goonies"))
        );


        // Range --------------------

        // Titles sorting after C (no end value)
        assertThat(
            indexRange(":movie/title", "C"),
            is(list("Commando", "Repo Man", "The Goonies")));

        // Titles sorting after Cu
        assertThat(
            indexRange(":movie/title", "Cu"),
            is(list("Repo Man", "The Goonies")));

        // Titles after D (no start value)
        assertThat(
            indexRange(":movie/title", "D"),
            is(list("Repo Man", "The Goonies")));

        // Titles after d - case-sensitivity regards small letters after capital letters
        assertThat(
            indexRange(":movie/title", "c"),
            is(empty()));

        // Titles before S
        assertThat(
            indexRange(":movie/title", null, "S"),
            is(list("Commando", "Repo Man")));

        // Titles before T ("The Goonies" is before "T")
        assertThat(
            indexRange(":movie/title", null, "T"),
            is(list("Commando", "Repo Man")));

        // Titles after C, before S
        assertThat(
            indexRange(":movie/title", "C", "S"),
            is(list("Commando", "Repo Man")));

        // Titles after D, before S
        assertThat(
            indexRange(":movie/title", "D", "S"),
            is(list("Repo Man")));


        // Limit --------------------

        assertThat(
            indexRange(":movie/title", null, null, 0, 0, 2),
            is(list("Commando", "Repo Man")));
        assertThat(
            indexRange(":movie/title", null, null, 0, 0, 1),
            is(list("Commando")));

        IllegalArgumentException limitCantBeZero = assertThrows(
            IllegalArgumentException.class,
            () -> indexRange(":movie/title", null, null, 0, 0, 0)
        );
        assertThat(
            limitCantBeZero.getMessage(),
            is(ErrorMsg.limit())
        );

        // Take all (!)
        assertThat(
            indexRange(":movie/title", null, null, 0, 0, -1),
            is(list("Commando", "Repo Man", "The Goonies")));


        // Offset --------------------

        assertThat(
            indexRange(":movie/title", null, null, 0, 0, 1000),
            is(list("Commando", "Repo Man", "The Goonies")));

        // Commando is skipped
        assertThat(
            indexRange(":movie/title", null, null, 0, 1, 1000),
            is(list("Repo Man", "The Goonies")));

        // Notice that offset is from current range (Repo Man is skipped)
        assertThat(
            indexRange(":movie/title", "D", null, 0, 1, 1000),
            is(list("The Goonies")));

        assertThat(
            indexRange(":movie/title", null, null, 0, 1, 1),
            is(list("Repo Man")));
    }

    @Test
    public void pull() {
        // Pull last movie
        Map entity = conn.db().pull("[*]", e3());
        assertThat(entity.get(read(":db/id")), is(e3()));
        assertThat(entity.get(read(":movie/title")), is("Repo Man"));
        assertThat(entity.get(read(":movie/genre")), is("punk dystopia"));
        assertThat(entity.get(read(":movie/release-year")), is(1984L));

        // dev-local in-memory db will pull within 1 ms
        if (!isDevLocal()) {
            Interrupted timedOut = assertThrows(
                Interrupted.class,
                () -> conn.db().pull("[*]", e3(), 1)
            );
            assertThat(timedOut.getMessage(), is("Datomic Client Timeout"));
        }
    }

    // since 1.0.61.65
    @Test
    public void indexPull() {
        // Pull from :avet index
        Iterator<Map<?, ?>> entities = (Iterator<Map<?, ?>>) conn.db().indexPull(
            ":avet",
            "[:movie/title :movie/release-year]",
            "[:movie/release-year 1985]"
        ).iterator();

        // 2 films pulled from index
        Map<?, ?> film1 = entities.next();
        Map<?, ?> film2 = entities.next();


        assertThat(film1.get(read(":movie/title")), is("The Goonies"));
        assertThat(film1.get(read(":movie/release-year")), is(1985L));

        assertThat(film2.get(read(":movie/title")), is("Commando"));
        assertThat(film2.get(read(":movie/release-year")), is(1985L));
    }
}
