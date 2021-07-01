package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ", (pickupTime: String) => ((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    // YOU NEED TO CHANGE THIS PART

    // register UDFs
    spark.udf.register("computeSpatialWeightSum", (x: Int, y: Int, z: Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int) =>
      HotcellUtils.getAdjacentHotCells(x: Int, y: Int, z: Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int))

    spark.udf.register("computeGetisOrdStat", (spatialWeightSumAndCellValue: Double, avgAttributeValue: Double, spatialWeightSum: Double, standardDeviation: Double, numOfCells: Double) =>
      HotcellUtils.computeGetisOrdStat(spatialWeightSumAndCellValue: Double, avgAttributeValue: Double, spatialWeightSum: Double, standardDeviation: Double, numOfCells: Double))

    // filter and count the "hot cell" from pickupInfo
    pickupInfo.createOrReplaceTempView("pickupView")
    val hotCellCountSql =
      s"""
         |SELECT x, y, z, COUNT(*) as cell_count
         |FROM pickupView
         |WHERE x >= $minX AND x <= $maxX
         |AND y >= $minY AND y <= $maxY
         |AND z >= $minZ AND z <= $maxZ
         |GROUP BY x, y, z
         |""".stripMargin
    val hotCellCountDf = spark.sql(hotCellCountSql)
    hotCellCountDf.createOrReplaceTempView("hotCellCountView")

    // average of measurements
    val xBarSumSql =
      """
        |SELECT SUM(cell_count)
        |FROM hotCellCountView
        |""".stripMargin
    val averageCellValue = spark.sql(xBarSumSql).first().getLong(0).toDouble / numCells

    // standard deviation of measurements
    val SSumSquaredSql =
      """
        |SELECT SUM(POW(cell_count, 2))
        |FROM hotCellCountView
        |""".stripMargin
    val standardDeviation = math.sqrt((spark.sql(SSumSquaredSql).first().getDouble(0) / numCells) - math.pow(averageCellValue, 2.0))

    val adjacentCellsSql =
      s"""
         |SELECT hcc2.x as x, hcc2.y as y, hcc2.z as z, SUM(hcc1.cell_count) as spatialWeightSumAndCellValue, computeSpatialWeightSum(hcc2.x,hcc2.y,hcc2.z,$minX,$minY,$minZ,$maxX,$maxY,$maxZ) as spatialWeightSum
         |FROM hotCellCountView as hcc1, hotCellCountView as hcc2
         |WHERE (hcc1.x = hcc2.x+1 OR hcc1.x = hcc2.x OR hcc1.x = hcc2.x-1)
         |AND (hcc1.y = hcc2.y+1 OR hcc1.y = hcc2.y OR hcc1.y = hcc2.y-1)
         |AND (hcc1.z = hcc2.z+1 OR hcc1.z = hcc2.z OR hcc1.z = hcc2.z-1)
         |GROUP BY hcc2.z, hcc2.y, hcc2.x
         |ORDER BY hcc2.z, hcc2.y, hcc2.x
         |""".stripMargin
    val adjacentCellsDf = spark.sql(adjacentCellsSql)
    adjacentCellsDf.createOrReplaceTempView("adjacentCellsView")

    val getisOrdStatSql =
      s"""
         |SELECT x, y, z, computeGetisOrdStat(spatialWeightSumAndCellValue,$averageCellValue,spatialWeightSum,$standardDeviation,$numCells) as getisOrdStat
         |FROM adjacentCellsView
         |ORDER BY getisOrdStat DESC
         |""".stripMargin
    val getisOrdStatDf = spark.sql(getisOrdStatSql)
    getisOrdStatDf.createOrReplaceTempView("getisOrdStatView")

    val hotCellAnalysisSql =
      """
        |SELECT x, y, z
        |FROM getisOrdStatView
        |""".stripMargin
    val hotCellAnalysisDf = spark.sql(hotCellAnalysisSql)
    hotCellAnalysisDf.createOrReplaceTempView("hotCellAnalysisView")

    hotCellAnalysisDf
  }
}