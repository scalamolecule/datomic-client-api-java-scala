package util

import datomicJavaScala.{SetupSpec, Setup}


class AdHoc extends SetupSpec {
  sequential

  "Ad hoc" in new Setup {

    ok
  }
}
