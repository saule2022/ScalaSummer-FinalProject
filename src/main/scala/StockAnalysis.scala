import SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, lag, lead}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.DataFrame

object StockAnalysis extends App {
  val spark = getSpark("Sparky")
  val filePath = "src/resources/stocks/stock_prices.csv"
  // val filePath = "src/resources/stocks/other_stock_data.csv"
  val df = readDataWithView(spark, filePath)

  // creating an array of distinct stocks/tickers
  val stocks = spark.sql(
    """
      |SELECT DISTINCT(ticker) FROM dfTable
      |""".stripMargin)

  val rows = stocks.collect()
  val strings = rows.map(_.getString(0))
  println(strings.mkString(","))

  //main loop
  for (st <- strings) {
    //preprocessing data
    val myDF = preprocessing(df, st)

    //building a model, predicting
    val rFormula = new RFormula()
      .setFormula("nextDayClose ~ open + high + low + close")

    val newDF = rFormula.fit(myDF).transform(myDF)

    val Array(train, test) = newDF.randomSplit(Array(0.8, 0.2))

    val linReg = new LinearRegression()
    val lrModel = linReg.fit(train)

    val predictDF = lrModel.transform(test)
    predictDF.show()

    //evaluating the model
    testLinearRegression(lrModel)

  }
  /**
   * Preprocessing step of Data
   *
   * @param df - Data frame
   * @param stockName-stock name
   * @return Data frame with particular stock data
   */

  def preprocessing(df: DataFrame, stockName: String): DataFrame = {
    val stock = df.where(col("ticker") === stockName)

    val windowSpec = Window.partitionBy("ticker").orderBy("date")
    val nextDayClose = lag(col("close"), -1).over(windowSpec)

    val stockDF = stock.withColumn("nextDayClose", nextDayClose)

    val stockDFNoNull = stockDF.na.drop
    stockDFNoNull
  }
  /**
   * tests Linear Regression Model
   *
   * @param model - LinearRegressionModel
   * @return returns Linear model parameters, intercept, coefficients, mean absolute error, mean square error
   */

  def testLinearRegression(model: LinearRegressionModel): Unit = {
    val intercept = model.intercept
    val coefficient = model.coefficients
    val mae = model.summary.meanAbsoluteError
    val mse = model.summary.meanSquaredError
    println(s"The model has following intercept: $intercept; coefficients: $coefficient; MAE: $mae; MSE: $mse")

  }

}