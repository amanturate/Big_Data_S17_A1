import org.apache.spark.sql.SparkSession

object Assignment_17_1 extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("example")
    .getOrCreate()

  val sc = spark.sparkContext
  //Loading the text file
  val csvDF1 = sc.textFile("C:/ACADGILD/Big Data/SESSION_17/17.1_datatset.txt")

  //Splitting the sentences
  val counts = csvDF1.flatMap(line => line.split("-"))

  println("Number of rows in the document = " + csvDF1.count())
  println("Total number of words in file = " + counts.collect.length)

}
