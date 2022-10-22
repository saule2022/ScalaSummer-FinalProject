# Scala_Spark_Summer_22
Scala 2 - Spark Big Data Analysis

Final Project - Stock Price Analysis
Stock Market Analysis
Assignment Objectives
The file stock_prices.csv contains the daily closing price of a few stocks on the NYSE/NASDAQ

Load up stock_prices.csv as a DataFrame
& Compute the average daily return of every stock for every date. Print the results to screen In other words, your output should have the columns:

date average_return yyyy-MM-dd return of all stocks on that date

Save the results to the file as Parquet(CSV and SQL are optional)

Which stock was traded most frequently - as measured by closing price * volume - on average?

Bonus Question
Which stock was the most volatile as measured by annualized standard deviation of daily returns?

Big Bonus: Build a model either trying to predict the next day's price(regression) or simple UP/DOWN/UNCHANGED? classificator. You can only use information information from earlier dates.
You can use other information if you wish from other sources for this predictor, the only thing what you can not do is use future data. smile

One idea would be to find and extract some information about particular stock and columns with industry data (for example APPL would be music,computers,mobile)

Do not expect something with high accuracy but main task is getting something going.

Good extra feature would be for your program to read any .CSV in this particular format and try to do the task. This means your program would accept program line parameters.

Assumptions

No dividends adjustments are necessary, using only the closing price to determine returns
If a price is missing on a given date, you can compute returns from the closest available date
Return can be trivially computed as the % difference of two prices



