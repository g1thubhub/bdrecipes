package module1.scala.catalyst

object Transformations extends App {
  import org.apache.spark.sql.catalyst.expressions._
  val firstExpr: Expression = UnaryMinus(Multiply(Subtract(Literal(11), Literal(2)), Subtract(Literal(9), Literal(5))))
  val transformed: Expression = firstExpr transformDown {
    case BinaryOperator(l, r) => Add(l, r)
    case IntegerLiteral(i) if i > 5 => Literal(1)
    case IntegerLiteral(i) if i < 5 => Literal(0)
  }

  println(firstExpr) // -((11 - 2) * (9 - 5))
  println(transformed) // -((1 + 0) + (1 + 5))
}