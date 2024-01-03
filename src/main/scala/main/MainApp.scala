import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._

object MainApp extends App {

    if (args.length != 1) {
      println("Usage: >spark-submit --class MainApp \n" +
        "--master local[*] --deploy-mode client \n" +
        "<jar-path> <output-file-path>")
      sys.exit(1)
    }

    val outputPath = args(0)

    val spark = SparkSession.builder()
      .appName("Spark Project")
      .master("local[*]")
      .getOrCreate()

    val queries = SparkQueries(spark, outputPath)

    // Provide the file paths for your data files and log file here
    val usersFilePath = "users.csv"
    val productsFilePath = "products.csv"
    val purchasesFilePath = "purchases.csv"
    val logFilePath = "log.txt"

    queries.shopActivity(usersFilePath, productsFilePath, purchasesFilePath, logFilePath)
    queries.siteMonitoring(logFilePath)
    queries.bonusQuery(usersFilePath, purchasesFilePath, logFilePath)

    spark.stop()

}