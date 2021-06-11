package cse512

import org.apache.spark.sql.SparkSession
import scala.math.sqrt

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=> {

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
          true
        }
      else
        {
          false
        }
    }
    )

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

//    println(resultDf.count())

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=> {

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
        true
      }
      else
      {
        false
      }
    })

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

//    println(resultDf.count())

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=> {

      val p1Split = pointString1.split(",")
      val p2Split = pointString2.split(",")

      val lat1 = p1Split(0).toDouble
      val long1 = p1Split(1).toDouble

      val lat2 = p2Split(0).toDouble
      val long2 = p2Split(1).toDouble

      val x = lat1 - lat2
      val y = long1 - long2

      val dist = sqrt((x * x) + (y * y))

      if (dist <= distance){
          true
      }
      else{
        false
      }

    })

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

//    println(resultDf.count())

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=> {

      val p1Split = pointString1.split(",")
      val p2Split = pointString2.split(",")

      val lat1 = p1Split(0).toDouble
      val long1 = p1Split(1).toDouble

      val lat2 = p2Split(0).toDouble
      val long2 = p2Split(1).toDouble

      val x = lat1 - lat2
      val y = long1 - long2

      val dist = sqrt((x * x) + (y * y))

      if (dist <= distance){
        true
      }
      else{
        false
      }
    })
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

//    println(resultDf.count())

    return resultDf.count()
  }
}
