import org.scalatest.Assertions._
import org.scalatest.funsuite.AnyFunSuite
import Main._
import scala.collection.mutable.ListBuffer

class MainTest extends AnyFunSuite {
  test("Testing concat list function") {
    val lst = concatList(ListBuffer("ar", "tk", "cn"), ListBuffer("cn", "be"))
    assert(lst.equals(ListBuffer("ar", "tk", "cn", "be")))
  }

  test("Testing get max run function: no uk") {
    val max_runs = getMaxRun(ListBuffer("ar", "tk", "cn", "be", "be", "ch", "il", "cg", "cn"), 0)
    assert(max_runs == 9)
  }

  test("Testing get max run function: uk at front") {
    val max_runs = getMaxRun(ListBuffer("uk","ar", "tk", "cn", "be", "be", "ch", "il", "cg", "cn"), 0)
    assert(max_runs == 9)
  }

  test("Testing get max run function: uk at end ") {
    val max_runs = getMaxRun(ListBuffer("ar", "tk","uk", "cn", "be", "be", "ch", "il", "cg", "cn","uk"), 0)
    assert(max_runs == 7)
  }

  test("Testing get max run function: uk ") {
    val max_runs = getMaxRun(ListBuffer("ar", "tk","uk", "cn", "be", "be", "ch", "il", "cg", "cn"), 0)
    assert(max_runs == 7)
  }

  test("Testing get all possible pairs in list – 3 elements") {
    val pairs = getPairs(ListBuffer("ar", "tk","uk"))
    assert(pairs == ListBuffer(("ar","tk"), ("ar","uk"),("tk","uk")))
  }

  test("Testing get all possible pairs in list – 1 element") {
    val pairs = getPairs(ListBuffer("ar"))
    assert(pairs == ListBuffer())
  }

  test("Testing get all possible pairs in list – 4 elements") {
    val pairs = getPairs(ListBuffer("ar", "tk","uk","ms"))
    assert(pairs == ListBuffer(("ar","tk"), ("ar","uk"),("ar","ms"),("tk","uk"),("tk","ms"),("uk","ms")))
  }
}
