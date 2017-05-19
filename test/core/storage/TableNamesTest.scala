package core.storage
import org.specs2.mutable.Specification

class TableNamesTest extends Specification {
  "test" in {
    val tn: TableNames = new TableNames()
    tn.values.length === 0
    tn.map.isEmpty === true

    tn.getOrAdd("user") === 0
    tn.getOrAdd("account") === 1
    tn.getOrAdd("res") === 2
    tn.values.length === 3
    tn.getOrAdd("account") === 1
    tn.values.length === 3
    tn.values === Vector("user", "account", "res")

    tn.get("user") === 0
    tn.get("user1") === -1
    tn.get("account") === 1
    tn.get("res") === 2
    tn.get("company") === -1

    tn.getOrAdd("company") === 3
    tn.values.length === 4

    tn.get("company") === 3
    tn.getOrAdd("company") === 3
    tn.values.length === 4
  }
}
