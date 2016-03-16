import org.locationtech.spatial4j.context.SpatialContext
import org.locationtech.spatial4j.context.jts.JtsSpatialContext
import org.locationtech.spatial4j.io.{ShapeReader, WKTReader}
import org.locationtech.spatial4j.shape.{SpatialRelation, Shape}

import scala.util.Try

object SpatialFilter {

  def parseDouble(s: String): Option[Double] = Try {
    s.toDouble
  }.toOption

  def parseWkt(wktString: String): Option[Shape] = Try {
    new WKTReader(JtsSpatialContext.GEO, null).parse(wktString)
  }.toOption

  def parsePoint(lng: Double, lat: Double): Option[Shape] = Try {
    val ctx: SpatialContext = SpatialContext.GEO
    ctx.makePoint(lng, lat)
  }.toOption

  def locatedIn(wktString: String, record: Map[String, String]): Boolean = {
    val values = List("dwc:decimalLatitude", "dwc:decimalLongitude") flatMap (record get)
    if (values.size == 2) {
      locatedInLatLng(wktString, values)
    } else {
      false
    }
  }


  def locatedInLatLng(wktString: String, values: Seq[String]): Boolean = {
    (parseDouble(values.head), parseDouble(values.last)) match {
      case (Some(lat), Some(lng)) =>
        (parseWkt(wktString), parsePoint(lng, lat)) match {
          case (Some(area), Some(point)) =>
            List(SpatialRelation.CONTAINS, SpatialRelation.INTERSECTS, SpatialRelation.WITHIN)
              .contains(area.relate(point))
          case _ => false
        }
      case _ => false
    }
  }
}
