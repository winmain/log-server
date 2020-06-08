package org.specs2

import org.specs2.execute.DecoratedResult
import org.specs2.matcher.{DataTable, DataTables, MatchResult}

trait DataTables2 extends DataTables {
  trait CheckedRow {
    def validate: MatchResult[Any]
  }

  /*
    Example usage:

    case class Row(email: String, valid: Boolean) extends CheckedRow {
      override def validate: MatchResult[Any] = EmailValidator.isValidInternetAddress(email) === valid
    }
    table1(
      Row("rus.shishkov@mail-ru", false),
      Row("katena.bauer.@mail.ru", true),
      Row("denischernyev97@mail", false))
   */

  def table1[R <: CheckedRow](rows: R*): DecoratedResult[DataTable] = {
    Table1[R](List("a row"), rows.view.map(DataRow1.apply).toList).executeRow(_.validate, exec = true)
  }
}
