# Java/Scala facade to datomic.client.api/api.async


This project contains thin Java and Scala facades to the 
[Datomic][datomic] Clojure Client api: 

- [datomic.client.api][sync]
- [datomic.client.api.async][async]

You can then use the various Datomic systems depending on the Client api from 
Java and Scala: 

- Dev local
- Peer Server
- Cloud

Using the facade in a live Cloud setting has not yet been tested. But since 
dev-local is equivalent and works fine, Cloud should too.

Note that this library is an independent release not associated with 
Datomic/Cognitect.

## Java/Scala

Code is written in Scala but compiles to the same bytecode as Java. 

Each language has been given its own exclusive namespace to ensure full 
compatibility and to accommodate for differences.

- In the `datomicJava` namespace all interfaces take java-compatible types as 
input and return only java-compatible types. 
- In the `datomicScala` namespace most types are java-compatible but where 
collections are iterated for instance, a Scala collection type is returned.
 

## Code organization 

The implementation has largely followed the structure of the 
[Datomic Java api][java api].

Clojure functions of the Client api taking a Connection object are encapsulated 
in a `Connection` class and so on. We end up with 4 classes and their methods:

- [Datomic][code Datomic] (similar to `Peer`)
  - `clientCloud` (providing AWSCredentialsProviderChain)
  - `clientCloud` (providing creds-profile name)
  - `clientDevLocal`
  - `clientPeerServer`
  - `q`
  - `qseq`
- [Client][code Client]: 
  - `administerSystem`
  - `connect`
  - `createDatabase`
  - `deleteDatabase`
  - `listDatabases`
- [Connection][code Connection]: 
  - `db`
  - `sync`
  - `transact`
  - `txRange`
  - `txRangeArray` (convenience method for populated Array)
  - `widh` (convenience method for single invocation)
  - `withDb`
- [Db][code Db]: 
  - `dbStats`
  - `asOf`
  - `since`
  - `with`
  - `history`
  - `datoms`
  - `indexRange`
  - `pull`
  - `indexPull`

Various helper classes are added too.

## Sync/async

Each language namespace has a `sync` and `async` package which
corresponds to the two Datomic client api sync/async versions. All the above 
methods are implemented for each package.


## Java async

The datomic client async api for Java generally returns a `CompletableFutue` of
a custom `Channel` type.

Once the Future has completed, chunked results can be retrieved by calling 
`chunk` one or more times on the `Channel` object until the Clojure Channels is empty.

Each chunk is a custom `Either` type that can be either a `Left` 
projection with a `CognitectAnomaly` or a `Right` projection containing the 
successful result of a type `T` for the operation in question. That way, the 
result can be type checked for an anomaly or a success.

When the Clojure Channel is empty, `Right(null)` is returned. Consuming code 
might therefore want to check for such terminating null value (when needed). 

- See [Java async tests][java async]


## Java sync

The datomic client sync api for Java returns the result as is (equivalent to
the type `T` of the async api).

Cognitect anomalies are thrown as runtime exceptions.

- See [Java sync tests][java sync]


## Scala async

The datomic client async api for Scala returns a `Future` of a `LazyList` of
chunks of data. The first (head) chunk of the `LazyList` is eargerly evaluated
and subsequent chunks can be retrieved lazily by simply looping the `LazyList`.

Each chunk in the `LazyList` is an `Either` of either a `Left[CognitectAnomaly]`
or a `Right[T]` where `T` is the main result type.

- See [Scala async tests][scala async]

## Scala sync

The datomic client sync api for Scala returns the result as is (equivalent to
the type `T` of the async api).

Cognitect anomalies are thrown as runtime exceptions.

- See [Scala sync tests][scala sync]


## Setup

This library presumes that you have a Datomic installation downloaded. It can
be a free/starter/pro version, although the free version is a bit behind and
won't provide all functionality. It's recommended to download an up-to-date
 version of starter/pro. On older versions of Datomic, some functionality might
 not be available. The `qseq` method was added in version 1.0.6165 for instance.

To use with dev-local, please download and install the [dev-tools][dev-tools].
Running dev-local doesn't require a transactor process to be running.

To use with peer-server, please start a [transactor][transactor] first and then 
a [Peer Server][peer-server].

Clone the project and open in your IDE to explore.
```
git clone https://github.com/scalamolecule/datomic-client-api-java-scala.git
```

## Testing

Run the Java tests by right-clicking on the `test.java.datomicJava.client` package
in the project view (in IntelliJ) and choose Run -> Tests in 'client' (or run 
individual tests similarly).

Run the Scala tests by right-clicking on the `test.scala.datomicScala.client` package
in the project view (in IntelliJ) and choose Run -> Specs2 in 'client' (or run 
individual tests similarly).


## Use with your project

This library is available on [Maven Central][maven].

Add Java dependency in POM file:
```
<dependency>
    <groupId>org.scalamolecule</groupId>
    <artifactId>datomic-client-api-java-scala</artifactId>
    <version>0.2.0</version>
</dependency>
```

Add Scala dependency in sbt build file:
```
libraryDependencies += "org.scalamolecule" % "datomic-client-api-java-scala" % "0.2.0"
```


## Author / License
By Marc Grue. Licensed under the [Apache License 2.0][apache2].


[datomic]: https://www.datomic.com
[java api]: https://docs.datomic.com/on-prem/javadoc/index.html

[sync]: https://docs.datomic.com/client-api/datomic.client.api.html
[async]: https://docs.datomic.com/client-api/datomic.client.api.async.html

[java sync]: https://github.com/scalamolecule/datomic-client-api-java-scala/tree/master/src/test/java/datomicJava/client/api/sync
[java async]: https://github.com/scalamolecule/datomic-client-api-java-scala/tree/master/src/test/java/datomicJava/client/api/sync
[scala sync]: https://github.com/scalamolecule/datomic-client-api-java-scala/tree/master/src/test/scala/datomicScala/client/api/sync
[scala async]: https://github.com/scalamolecule/datomic-client-api-java-scala/tree/master/src/test/scala/datomicScala/client/api/async

[code Datomic]: https://github.com/scalamolecule/datomic-client-api-java-scala/blob/master/src/main/scala/datomicScala/client/api/sync/Datomic.scala
[code Client]: https://github.com/scalamolecule/datomic-client-api-java-scala/blob/master/src/main/scala/datomicScala/client/api/sync/Client.scala
[code Connection]: https://github.com/scalamolecule/datomic-client-api-java-scala/blob/master/src/main/scala/datomicScala/client/api/sync/Client.scala
[code Db]: https://github.com/scalamolecule/datomic-client-api-java-scala/blob/master/src/main/scala/datomicScala/client/api/sync/Db.scala
[dev-tools]: https://docs.datomic.com/cloud/dev-local.html
[peer-server]: https://docs.datomic.com/on-prem/peer-server.html
[transactor]: https://docs.datomic.com/on-prem/transactor.html
[apache2]: http://en.wikipedia.org/wiki/Apache_license
[maven]: https://repo1.maven.org/maven2/org/scalamolecule/datomic-client-api-java-scala/