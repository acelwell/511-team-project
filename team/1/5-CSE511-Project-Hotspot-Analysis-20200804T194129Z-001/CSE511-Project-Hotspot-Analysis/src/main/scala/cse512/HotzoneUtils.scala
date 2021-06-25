package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val queryRectangleSplit = queryRectangle.split(",")
    val pointStringSplit = pointString.split(",")

    val lat1 = queryRectangleSplit(0).toFloat
    val long1 = queryRectangleSplit(1).toFloat
    val lat2 = queryRectangleSplit(2).toFloat
    val long2 = queryRectangleSplit(3).toFloat

    val plat1 = pointStringSplit(0).toFloat
    val plong1 = pointStringSplit(1).toFloat

    if(((lat1 <= plat1 && plat1 <= lat2) || (lat2 <= plat1 && plat1 <= lat1)) &&
      ((long1 <= plong1 && plong1 <= long2) || (long2 <= plong1 && plong1 <= long1)))
    {
      return true
    }
    else
    {
      return false
    }
  }

  // YOU NEED TO CHANGE THIS PART

}
