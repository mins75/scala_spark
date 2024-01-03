import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class SparkQueries(private val spark: SparkSession
                  , private val outputPath: String) {

  private def readCSV(filePath: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load(filePath)
  }

  // Utility function to save DataFrame to a CSV file
  private def saveToCSV(df: DataFrame, fileName: String, output: String): Unit = {
    df.write
      .format("csv")
      .option("header", "true")
      .option("sep", ",")
      .save(s"$output/$fileName")
  }

  def shopActivity(usersFilePath: String, productsFilePath: String, purchasesFilePath: String, logFilePath: String): Unit = {
    import spark.implicits._

    // Load data from CSV files
    val usersDF = readCSV(usersFilePath)
    val productsDF = readCSV(productsFilePath)
    val purchasesDF = readCSV(purchasesFilePath)

    // Calculate age of clients
    val currentDate = current_date()
    val ageDF =
      usersDF.select($"id_user", $"date_of_birth", (datediff(currentDate, to_date($"date_of_birth", "yyyy-MM-dd")) / 365.25).as("age"))

    // Query 1: What are the mean, min, max age of clients?
    val ageStatsDF = ageDF.agg(mean("age").as("mean_age"), min("age").as("min_age"), max("age").as("max_age"))

    // Query 2: How many clients are there per country?
    val clientsPerCountryDF = usersDF.groupBy("country").agg(count("*").as("num_clients"))

    // Query 3.1: How many purchases are there per client?
    val purchasesPerClientDF = purchasesDF.groupBy("id_user").count()
    // Query 3.2: How many purchases are there per country?
    val purchasesPerCountryDF = purchasesDF
      .join(usersDF, Seq("id_user"))
      .groupBy("country")
      .agg(count("*").as("num_purchases"))

    // Query 4: How much money each client has spent in total?
    val totalSpentPerClientDF = purchasesDF
      .join(productsDF, Seq("id_product"))
      .join(usersDF, Seq("id_user"))
      .groupBy("id_user")
      .agg(sum("price").as("total_spent"))

    // Query 5: What proportion of visits are made by people not subscribed to the website?
    val logDF = spark.read.text(logFilePath)
    val nonSubscribedVisitsDF = logDF.filter(col("value").contains("UNK"))
    val totalVisits = logDF.count()
    val nonSubscribedProportionDF = nonSubscribedVisitsDF.agg(count("*").as("non_subscribed_visits"), lit(totalVisits).as("total_visits"))
      .withColumn("proportion", $"non_subscribed_visits" / $"total_visits")

    // Query 6: How many times a client looked at each product they bought?
    val productViewsPerPurchaseDF = logDF
      .selectExpr("substring(value, instr(value, 'Visitor : ') + 10, instr(value, ' - Product') - (instr(value, 'Visitor : ') + 10)) as visitor_id",
        "substring(value, instr(value, 'Product : ') + 10, instr(value, '. Response') - (instr(value, 'Product : ') + 10)) as product_id")
      .groupBy("visitor_id", "product_id")
      .agg(count("*").as("num_views_per_product"))

    // Save results to CSV
    saveToCSV(ageStatsDF, "age_stats", outputPath)
    saveToCSV(clientsPerCountryDF, "clients_per_country", outputPath)
    saveToCSV(purchasesPerClientDF, "purchases_per_client", outputPath)
    saveToCSV(purchasesPerCountryDF, "purchases_per_country", outputPath)
    saveToCSV(totalSpentPerClientDF, "total_spent_per_client", outputPath)
    saveToCSV(nonSubscribedProportionDF, "non_subscribed_proportion", outputPath)
    saveToCSV(productViewsPerPurchaseDF, "product_views_per_purchase", outputPath)
  }

  def siteMonitoring(logFilePath: String): Unit = {
    // Load log data from CSV file
    val logDF = spark.read.text(logFilePath)

    val logCleanDF = logDF.select(
      to_timestamp(substring(col("value"), 1, 19), "yyyy-MM-dd HH:mm:ss").as("date"),
      regexp_extract(col("value"), "Visitor : (\\d+)", 1).cast("int").as("id_user"),
      regexp_extract(col("value"), "Product : (\\d+)", 1).cast("int").as("product_id"),
      regexp_extract(col("value"), "Response time : (\\d+)ms", 1).cast("int").as("response_time"),
      when(col("value").contains("ERROR :"), regexp_extract(col("value"), "ERROR : (.+)", 1)).otherwise(null).as("err")
    )

    // Query 1: What are the mean, min, max response times per date?
    val responseTimesDF = logDF
      .select(
        to_date(substring(col("value"), 1, 10), "yyyy-MM-dd").as("date"),
        regexp_extract(col("value"), "Response time : (\\d+)ms", 1).cast("int").as("response_time")
      )

    val meanMinMaxResponseTimesDF = logCleanDF
      .groupBy(to_date(col("date"), "yyyy-MM-dd").as("date_clean"))
      .agg(
        mean("response_time").as("mean_response_time"),
        min("response_time").as("min_response_time"),
        max("response_time").as("max_response_time")
      )

    // Query 2: How many errors are there per date?
    val errorsPerDateDF = logCleanDF
      .filter(col("err").isNotNull)
      .groupBy(to_date(col("date"), "yyyy-MM-dd").as("date_clean"))
      .agg(count("*").as("num_errors"))

    // Save results to CSV
    saveToCSV(meanMinMaxResponseTimesDF, "mean_min_max_response_times", outputPath)
    saveToCSV(errorsPerDateDF, "errors_per_date", outputPath)
  }

  // See below the bonus queries

  def bonusQuery(usersFilePath: String, purchasesFilePath: String, logFilePath: String): Unit = {
    import spark.implicits._

    //load files
    val purchasesDF = readCSV(purchasesFilePath)
    val usersDF = readCSV(usersFilePath)
    val logDF = spark.read.text(logFilePath)
    val logCleanDF = logDF.select(
      to_timestamp(substring(col("value"), 1, 19), "yyyy-MM-dd HH:mm:ss").as("date"),
      regexp_extract(col("value"), "Visitor : (\\d+)", 1).cast("int").as("id_user"),
      regexp_extract(col("value"), "Product : (\\d+)", 1).cast("int").as("id_product"),
      regexp_extract(col("value"), "Response time : (\\d+)ms", 1).cast("int").as("response_time"),
      when(col("value").contains("ERROR :"), regexp_extract(col("value"), "ERROR : (.+)", 1)).otherwise(null).as("err")
    )

    // First query : Compare the average response time per user on the country window
    val userAvgResponseTimeDF = logCleanDF.groupBy("id_user").agg(avg("response_time").as("avg_response_time_per_user"))

    // Inner join with users
    val logWithUserCountryDF = userAvgResponseTimeDF.join(usersDF, Seq("id_user"), "inner")

    // Calculate statistics on the country level
    val countryStatsWindow = Window.partitionBy("country")
    val responseTimePerCountry = logWithUserCountryDF
      .withColumn("avg_response_time_per_country", avg("avg_response_time_per_user").over(countryStatsWindow))
      .withColumn("response_time_mean_deviation", abs(col("avg_response_time_per_user") - col("avg_response_time_per_country")))

    // Second query : Compare the first date at which an user looked at a product and its first purchase of this product
    val minPurchaseDatesDF = purchasesDF
      .groupBy("id_user")
      .agg(min("date_of_purchase").as("min_purchase_date"))

    val firstLookDF = logCleanDF
      .groupBy("id_user")
      .agg(min("date").as("first_look_date"))

    // Inner join on the id_user
    val logAndPurchaseDF = firstLookDF.join(minPurchaseDatesDF, Seq("id_user"), "inner")

    // Calculate the time it takes for users to buy a product
    val timeToBuyDF = logAndPurchaseDF
      .withColumn("time_to_buy_product", datediff(col("min_purchase_date"), col("first_look_date")))
      .orderBy("id_user")

    // Third query : Compare the age of each user with the average age of a country
    val currentDate = current_date()
    val ageDF =
      usersDF.select($"id_user", $"country", $"date_of_birth", (datediff(currentDate, to_date($"date_of_birth", "yyyy-MM-dd")) / 365.25).as("age"))

    val ageDeviationUsersDF = ageDF
      .withColumn("avg_age_by_country", avg("age").over(Window.partitionBy("country")))
      .withColumn("age_mean_deviation", abs($"age" - $"avg_age_by_country"))

    // Save results to CSV

    saveToCSV(responseTimePerCountry, "bonus_response_time_per_country", outputPath)
    saveToCSV(timeToBuyDF, "bonus_time_to_buy_product", outputPath)
    saveToCSV(ageDeviationUsersDF, "bonus_age_deviation", outputPath)

  }
}

object SparkQueries {
  def apply(spark: SparkSession, outputPath: String): SparkQueries = new SparkQueries(spark, outputPath)
}
