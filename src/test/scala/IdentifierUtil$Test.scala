import org.apache.spark.graphx.Edge
import org.apache.spark.sql._
import org.scalatest.{FlatSpec, Matchers}

class IdentifierUtil$Test extends FlatSpec with Matchers {

  "a row" should "be transformed into a list of links" in {
    val rows = Seq(Row("src1", "refers", "dst1"),
      Row("src2", "refers", "dst1"),
      Row("src3", "refers", "dst2"))

    val rawEdges = rows.flatMap(IdentifierUtil.toEdge)

    rawEdges should contain(Edge(1768280580, 557222360, "refers"))

    val vertices = rows.flatMap {
      IdentifierUtil.toVertices
    } distinct

    vertices should contain((1768280580, "src1"))

  }

  "a row with nulls" should "be ignored" in {
    val rows = Seq(Row(null, "refers", "dst1"),
      Row("src2", "refers", "dst1"),
      Row("src3", "refers", "dst2"))

    val rawEdges = rows.flatMap(IdentifierUtil.toEdge)
    rawEdges should contain(Edge(-725793238, -328124296, "refers"))

    val vertices = rows.flatMap {
      IdentifierUtil.toVertices
    } distinct

  }


}
