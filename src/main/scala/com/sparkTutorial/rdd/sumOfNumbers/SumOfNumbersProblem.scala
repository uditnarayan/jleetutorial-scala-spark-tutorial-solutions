package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */
    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("SumOfNumbers").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("in/prime_nums.text")
    val numbers = data.flatMap(line => line.split("\\s+"))

    // I missed this in my initial solution and it causes
    // issues as there are empty string in the RDD.
    val filtered = numbers.filter(num => !num.isEmpty)

    val integers = filtered.map(num => num.toInt)
    val sum = integers.reduce((a,b) => a+b)

    println("Sum: " + sum)
  }
}
