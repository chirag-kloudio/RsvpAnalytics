import org.apache.spark.{SparkContext, SparkConf} // Spark
import com.datastax.spark.connector._ // Cassandra connector
import scala.collection.immutable.ListMap

import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkAnalysis {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    // Configuration
    val conf = new SparkConf(true)
      .setAppName("Rsvp Analysis")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "127.0.0.1")

    val sc = new SparkContext(conf) // Create SparkContext

    // Read the data from cassandra
    val rsvp = sc.cassandraTable("meetup", "rsvpstream")
    rsvp.cache() // cache the RDD

    val group_city_data = rsvp.select("group_city") // Get group city column data
    group_city_data.cache() // cache group city data

    //// Overall no.of of unique group cities

    // Get distinct group_city names
    val groupCityOverall = group_city_data
      .map(city => city.getString(0)).distinct()
    // Calculate the count of the distinct city names
    val groupCityOverallCount = groupCityOverall.count()

    // Map to (group_city, 1)
    val group_city_count = group_city_data
      .map(city => (city.getString(0), 1))

    //// Count of each group cities. ex: New York - 5059, San Fransisco - 3125

    // Calculate the count for each city
    val groupCityCounts = group_city_count.reduceByKey(_ + _)
    //Sort by value in ascending order
    val groupCityCountsOverall = ListMap(groupCityCounts.collect.toSeq.sortWith(_._2 > _._2):_*)

    //// Overall no. of group countries
    val groupCountrydata = rsvp.select("group_country")
    groupCountrydata.cache()
    val groupCountriesOverall = groupCountrydata
      .map(country => country.getString(0)).distinct()
    val groupCountriesOverallCount = groupCountriesOverall.count()

    // Map to (group_country, 1)
    val groupCountryCount = groupCountrydata
      .map(country => (country.getString(0), 1))
      .reduceByKey(_ + _)

    val groupCountryCountsOverall = ListMap(groupCountryCount.collect.toSeq.sortWith(_._2 > _._2):_*)
    //groupCountryCountsOverall.foreach(println)

    // Counts of responses - yes or no
    val response = rsvp.select("response")
    response.cache()
    val responseCount = response
      .map(resp => (resp.getString(0), 1))
      .reduceByKey(_ + _)

    val responseOverallCount = ListMap(responseCount.collect.toSeq.sortWith(_._2 > _._2):_*)
    //responseOverallCount.foreach(println)
    val yesCount = responseOverallCount("yes")
    val noCount = responseOverallCount("no")
    val totalCount = yesCount + noCount

    // Calculate yes & no proportions
    val yesProportion = (yesCount * 100) / totalCount
    val noProportion = (noCount * 100) / totalCount

    //// response per country
    val responseCountry = rsvp.select("group_country", "response")
    val groupedResponses = responseCountry
      .map(line => (line.getString(0), line.getString(1)))
      .countByValue

    // Users from which countries invite guests with them
    val countryGuests = rsvp.select("group_country", "guests")
      .map(line => (line.getString(0), line.getString(1).toInt))
      .filter(line => line._2 > 0)
      .reduceByKey(_ + _)

    // Total count of countries which invited guests
    val countCountryGuests = countryGuests
      .map(country => country._1).distinct().count()

    // Top 10 countries in desceding order of the no.of guests invited
    val topCountrieswithGuests = ListMap(countryGuests.collect.toSeq.sortWith(_._2 > _._2):_*)
    //topCountrieswithGuests.take(10).foreach(println)


    // Users from which countries invite guests with them
    val cityGuests = rsvp.select("group_city", "guests")
      .map(line => (line.getString(0), line.getString(1).toInt))
      .filter(line => line._2 > 0)
      .reduceByKey(_ + _)

    // Total count of cities which invited guests
    val countCityGuests = cityGuests
      .map(city => city._1).distinct().count()

    // Top 10 countries in desceding order of the no.of guests invited
    val topCitywithGuests = ListMap(cityGuests.collect.toSeq.sortWith(_._2 > _._2):_*)
    //topCitywithGuests.take(10).foreach(println)
    println(countCityGuests)

  }
}
