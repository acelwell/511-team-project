package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
  
  val rectangle_point = queryRectangle.split(",")
  val target_point = pointString.split(",")

  val xTargetLong: Double = target_point(0).trim.toDouble
  val yTargetLat: Double = target_point(1).trim.toDouble
  val minLongX: Double = math.min(rectangle_point(0).trim.toDouble, rectangle_point(2).trim.toDouble)
  val maxLongX: Double = math.max(rectangle_point(0).trim.toDouble, rectangle_point(2).trim.toDouble)
  val minLatY: Double = math.min(rectangle_point(1).trim.toDouble, rectangle_point(3).trim.toDouble)
  val maxLatY: Double = math.max(rectangle_point(1).trim.toDouble, rectangle_point(3).trim.toDouble)

  if ((xTargetLong >= minLongX) && (xTargetLong <= maxLongX) && (yTargetLat >= minLatY) && (yTargetLat <= maxLatY)) {
    return true
  }
  return false
  }
}