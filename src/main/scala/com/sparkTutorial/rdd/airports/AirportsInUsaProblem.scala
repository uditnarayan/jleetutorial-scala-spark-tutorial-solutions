package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsInUsaProblem {
  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
       and output the airport's name and the city's name to out/airports_in_usa.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "Putnam County Airport", "Greencastle"
       "Dowagiac Municipal Airport", "Dowagiac"
       ...
     */

    val sparkConf = new SparkConf().setAppName("airports").setMaster("local[1]")
    val sparkContext = new SparkContext(sparkConf)

    val airports = sparkContext.textFile("in/airports.text")
    val usAirports = airports.filter(
      line => line.split(Utils.COMMA_DELIMITER)(3).equals("\"United States\""))

    val usAirportNameAndCity = usAirports.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + ", " + splits(2)
    })
    usAirportNameAndCity.saveAsTextFile("out/airports_in_usa_soln")
  }
}
