import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer

object Main {

  // start spark session
  val spark = SparkSession
    .builder()
    .appName("flight-data")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  // function to read data from csv files
  def read_data(path: String) = {
    spark.read.option("Header", true).csv(path)
  }

  // function to concat flight journeys of a single passenger
  def concatList(a: ListBuffer[String], b: ListBuffer[String]): ListBuffer[String] = {
    if (a.last == b.head) {
      a ++= b.tail
      a
    }
    else a++b
  }

  // function to get the longest run without landing in uk
  def getMaxRun(flight_list: ListBuffer[String], max_run: Int) : Int = {
    if (flight_list.isEmpty) max_run
    else {

      var current_run = flight_list.takeWhile(_ != "uk")
      flight_list --= current_run
      if (!flight_list.isEmpty) {
        flight_list -= flight_list.head
      }
      var new_max_run = if (max_run > current_run.length) max_run else current_run.length
      getMaxRun(flight_list, new_max_run)
    }
  }

  def getPairs(passengers: ListBuffer[String]): ListBuffer[(String, String)] = {
    def getPairs(passengers: ListBuffer[String], pairs : ListBuffer[(String,String)]): ListBuffer[(String, String)] = {
      if (passengers.isEmpty) pairs
      else {
        val curr = passengers.head
        passengers -= passengers.head
        val new_pairs = for (x <- passengers) yield (curr, x)
        pairs ++= new_pairs
        getPairs(passengers, pairs)
      }
    }
    getPairs(passengers, new ListBuffer())
  }


  def main(args: Array[String]) = {

    val debug = true // set to true to see console output

    val flight_path = "flightData.csv"
    val passengers_path = "passengers.csv"

    // read data
    val flight_df = read_data(flight_path)
    val passengers_df = read_data(passengers_path)

    if (debug) {
      flight_df.show()
      passengers_df.show()
    }

    // Question 1: Find the total number of flights for each month
    val number_of_flights = flight_df.withColumn("Month", month(col("date")))
      .groupBy("Month")
      .agg(count("*").alias("Number of Flights"))
      .orderBy("Month")

    if (debug) {
      number_of_flights.show()
    }

    number_of_flights.write.option("header",true).mode(SaveMode.Overwrite).csv("q1")

    // Question 2: Find the names of the 100 most frequent flyers
    val frequent_flyers = flight_df.groupBy("passengerID")
      .agg(count("*").alias("Number of Flights"))
      .orderBy(desc("Number of Flights"))
      .limit(100)
      .toDF()

    if (debug) {
      frequent_flyers.show()
    }

    val frequent_flyers_names = frequent_flyers.join(passengers_df, "passengerID")
      .withColumnRenamed("passengerID","Passenger_ID")
      .withColumnRenamed("firstName","First_Name")
      .withColumnRenamed("lastName","Last_Name")
      .withColumnRenamed("Number of Flights","Number_of_Flights")
      .orderBy(desc("Number_of_Flights"))

    if (debug) {
      frequent_flyers_names.show()
    }

    frequent_flyers_names.write.option("header",true).mode(SaveMode.Overwrite).format("csv").save("q2")

     /* Question 3: Find the greatest number of countries a passenger has been in without being in the UK.
     For example, if the countries a passenger was in were: UK -> FR -> US -> CN -> UK -> DE -> UK,
     the correct answer would be 3 countries. */

   val longest_run = flight_df.sort("date").rdd
      .map(row => (row.get(0).toString(), ListBuffer(row.get(2).toString(),row.get(3).toString())))
      .reduceByKey(concatList(_,_))
      .mapValues(getMaxRun(_,0)).collect() // return type is Array[(String, String)]

    import spark.implicits._ // to convert rdd to df
    val longest_runDF = spark.sparkContext.parallelize(longest_run).toDF("Passenger Id","Longest Run")

    if (debug) {
      longest_runDF.show()
    }

    longest_runDF.write.option("header",true).mode(SaveMode.Overwrite).csv("q3")

    /* Question 4: Find the passengers who have been on more than 3 flights together.
        - extract customers grouped by flight. (key, value) -> (flight_id, ListBuffer(passengers))
        - get all possible combinations of passengers for each flight (no duplicates)
        - with (pair, 1), sum the values to get total co-occurences  */

    val passenger_byflight = flight_df.rdd
      .map(row => (row.get(1).toString(),ListBuffer(row.get(0).toString())))
      .reduceByKey(concatList(_,_))
      .map(row => getPairs(row._2))
      .collect()
      .flatten

    val passenger_coccurences = spark.sparkContext.parallelize(passenger_byflight)
      .map(row => (row, 1))
      .reduceByKey(_+_)
      .filter(row => row._2 > 3)
      .map(row => (row._1._1, row._1._2,row._2))
      .collect()

    val passenger_coccurences_df = spark.sparkContext.parallelize(passenger_coccurences)
      .toDF("Passenger 1 ID","Passenger 2 ID","Number of flights together")

    if (debug) {
      passenger_byflight.take(5).foreach(println)
      passenger_coccurences.take(5).foreach(println)
      passenger_coccurences_df.show()
    }

    passenger_coccurences_df.write.option("header",true).mode(SaveMode.Overwrite).csv("q4")
    spark.stop()

  }

}
