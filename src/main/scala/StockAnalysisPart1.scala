import SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object StockAnalysisPart1 extends App {

  for ((arg, i) <- args.zipWithIndex) {
    println(s" argument No. $i, argument: $arg")
  }
  if (args.length >= 1) {
    println()
  }

  val filePath = "src/resources/csv/stock_prices_.csv"
  val src = if (args.length >= 1) args(0) else filePath

  println(s"My Source file will be $src")

  // setting up Spark
  val spark = getSpark("Sparky")

  //Load up stock_prices.csv as a DataFrame
  val df = readDataWithView(spark, src)

  // Compute the average daily return of every stock for every date
  val dfWithReturn = df.withColumn("dailyReturn", expr("round((close - open)/open * 100, 2)"))

  val windowSpec = Window
    .partitionBy("ticker")
    .orderBy(col("date").asc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  val avgDailyReturn = avg(col("dailyReturn")).over(windowSpec)
  val cumDailyReturn = sum(col("dailyReturn")).over(windowSpec)

  val dfWithDifReturns = dfWithReturn.select(col("*"),
    round(avgDailyReturn, 2).alias("avgDailyReturn"),
    round(cumDailyReturn, 2).alias("totalReturn"))

  println("Table1. Date, open, high, low, close prices, daily return, average and cumulative return of 5 stocks each day:")
  dfWithDifReturns.show(10)

  // Save the results to the file as Parquet(CSV and SQL are optional)
  dfWithDifReturns.write
    .format("parquet")
    .mode("overwrite")
    .save("src/resources/final/my-parquet-file.parquet")

  dfWithDifReturns.write
    .format("csv")
    .mode("overwrite")
    .option("sep", "\t")
    .option("header", true)
    .save("src/resources/final/my-csv-file.csv")

  // this option shows average return of all stocks each day and total return of 5 stocks each day
  println("Table2. Date, average return, total return of stocks per day:")
  df.withColumn("daily_return", expr("round((close - open)/open * 100, 2)"))
    .groupBy("date")
    .agg(expr("round(avg(daily_return),2)").alias("Average Return per Stock"),
      expr("round(sum(daily_return), 2)").alias("Total Return of all Stocks")
    )
    .orderBy("date")
    .show(10)

  //Which stock was traded most frequently - as measured by closing price * volume - on average?

  val tradeFrequency = df.withColumn("frequency", expr("close * volume"))
    .groupBy("ticker")
    .agg(avg("frequency").alias("avgTradingFrequency"))
    .orderBy(desc("avgTradingFrequency"))

  println("Table3. Trading frequency of stocks, based on closing price * volume:")
  tradeFrequency.show()

  val mostTraded = tradeFrequency.collect().map(_.getString(0))
  println(s"${mostTraded.head} stock was the most frequently traded throughout given period. \n")

  //Which stock was the most volatile as measured by annualized standard deviation of daily returns?
  println("Calculating the volatility of stocks for given period:")

  val SDForDailyReturn = stddev(col("dailyReturn")).over(windowSpec)

  val dfVolatile = dfWithReturn.select(col("*"),
    round(SDForDailyReturn, 2).alias("stDevOfDailyReturn"))

  //counting how many periods/dates to calculate volatility
  val dateCount = df.select(countDistinct("date")).collect().map(_.toSeq)
  val dateCountInt = dateCount.head.head
  println(s"The total number of trading days is: $dateCountInt \n")

  val dfVolatileWithDays = dfVolatile.withColumn("dateCount", lit(dateCountInt))

  val maxDate = df.agg(max("date")).collect().map(_.getString(0)).head
  println(s"Last day of the period is: $maxDate \n")

  val dfYearlyVolatility = dfVolatileWithDays.where(col("date") === maxDate)
    .withColumn("volatility", expr("round(stDevOfDailyReturn * sqrt(dateCount), 2)"))
    .orderBy(desc("volatility"))

  println("Table3. Accumulated Standard Deviation and Volatility for all stocks for given period:")
  dfYearlyVolatility.show()

  val volatilityArray = dfYearlyVolatility.collect().map(_.toSeq)
  val highestVolatility = volatilityArray.head

  println(s"${highestVolatility(6)} Stock was the most volatile throughout given period: ${highestVolatility.last}")


}