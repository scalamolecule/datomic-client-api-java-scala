package util

import datomic.Peer
import org.specs2.mutable.Specification


class PeerServerReset extends Specification {
  sequential

  // 1. Stop Peer Server process (ctrl-c)
  // 2. Run both tests here to reset Peer Server `hello` db

  "Recreate hello db" >> {
    // Run this test only to delete test db 'hello'
    Peer.deleteDatabase("datomic:dev://localhost:4334/hello")
    Peer.createDatabase("datomic:dev://localhost:4334/hello")
    ok
  }

  // 3. Start Peer Server again:
  //    > bin/run -m datomic.peer-server -a k,s -d hello,datomic:mem://hello
  //    or
  //    > bin/run -m datomic.peer-server -h localhost -p 8998 -a k,s -d hello,datomic:dev://localhost:4334/hello
}
