import com.spatial4j.core.context.SpatialContext
import com.spatial4j.core.shape.{SpatialRelation, Shape}

import scala.util.Try

object SpatialFilter {

  def parseDouble(s: String): Option[Double] = Try {
    s.toDouble
  }.toOption

  def parseWkt(wktString: String): Option[Shape] = Try {
    val ctx: SpatialContext = SpatialContext.GEO
    ctx.readShapeFromWkt(wktString)
  }.toOption

  def parsePoint(lng: Double, lat: Double): Option[Shape] = Try {
    val ctx: SpatialContext = SpatialContext.GEO
    ctx.makePoint(lng, lat)
  }.toOption

  def locatedIn(wktString: String, record: Map[String, String]): Boolean = {
    val values = List("dwc:decimalLatitude", "dwc:decimalLongitude") flatMap (record get)
    if (values.size == 2) {
      (parseDouble(values.head), parseDouble(values.tail.head)) match {
        case (Some(lat), Some(lng)) =>
          (parseWkt(wktString), parsePoint(lng, lat)) match {
            case (Some(area), Some(point)) =>
              List(SpatialRelation.CONTAINS, SpatialRelation.INTERSECTS, SpatialRelation.WITHIN)
                .contains(area.relate(point))
            case _ => false
          }
        case _ => false
      }
    } else {
      false
    }
  }


}
