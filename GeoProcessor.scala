package questions

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD


/** GeoProcessor provides functionalities to
  * process country/city/location data.
  * We are using data from http://download.geonames.org/export/dump/
  * which is licensed under creative commons 3.0 http://creativecommons.org/licenses/by/3.0/
  *
  * @param sc reference to SparkContext
  * @param filePath path to file that should be modified
  */
class GeoProcessor(sc: SparkContext, filePath:String) {

  //read the file and create an RDD
  val file = sc.textFile(filePath)

  /** filterData removes unnecessary fields and splits the data so
    * that the RDD looks like RDD(Array("<name>","<countryCode>","<dem>"),...))
    * Fields to include:
    *   - name
    *   - countryCode
    *   - dem (digital elevation model)
    *
    * @return RDD containing filtered location data. There should be an Array for each location
    */
  def filterData(data: RDD[String]) = {
    /* hint: you can first split each line into an array.
    * Columns are separated by tab ('\t') character.
    * Finally you should take the appropriate fields.
    * Function zipWithIndex might be useful.
    */

    val t1 = data.map( line => line.split('\t'))
    val t2 = t1.map(x => x.zipWithIndex)
    t2.map(x => Array(x(1)._1, x(8)._1, x(16)._1))
  }

  /** filterElevation is used to filter to given countryCode
    * and return RDD containing only elevation(dem) information
    *
    * @param countryCode code e.g(AD)
    * @param data an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD containing only elevation information
    */
  def filterElevation(countryCode: String,data: RDD[Array[String]]): RDD[Int] = {

    val filtRows = data.filter(x => x(1) == countryCode)
    filtRows.map(x => x(2).toInt)
  }


  /** elevationAverage calculates the elevation(dem) average
    * to specific dataset.
    *
    * @param data: RDD containing only elevation information
    * @return The average elevation
    */
  def elevationAverage(data: RDD[Int]): Double = {

    val elev_sum = data.reduce(_+_)
    elev_sum.toDouble/data.count()
  }

  /** mostCommonWords calculates what is the most common
    * word in place names and returns an RDD[(String,Int)]
    * You can assume that words are separated by a single space ' '. 
    *
    * @param data an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD[(String,Int)] where string is the word and Int number of 
    * occurrences. RDD should be in descending order (sorted by number of occurrences).
    * e.g ("hotel", 234), ("airport", 120), ("new", 12)
    */
  def mostCommonWords(data: RDD[Array[String]]): RDD[(String,Int)] = {

    val allWords = data.flatMap(x => x(0).split(" "))
    val wc_des = allWords.map((_,1)).reduceByKey(_+_).collect().sortWith(_._2 > _._2)
    sc.parallelize(wc_des)
  }

  /** mostCommonCountry tells which country has the most
    * entries in geolocation data. The correct name for specific
    * countrycode can be found from countrycodes.csv.
    *
    * @param data filtered geoLocation data
    * @param path to countrycode.csv file
    * @return most common country as String e.g Finland or empty string "" if countrycodes.csv
    *         doesn't have that entry.
    */
  def mostCommonCountry(data: RDD[Array[String]], path: String): String = {

    val countryCodes = data.map(x => x(1))
    val countryCodes_des = countryCodes.map((_, 1)).reduceByKey(_ + _).collect().sortWith(_._2 > _._2)
    val most_com_cc = countryCodes_des(0)._1

    val cc_file = sc.textFile(path)
    val filt_country = cc_file.map(x => x.split(",")).filter(y => y(1) == most_com_cc)
    if(filt_country.isEmpty) "" else filt_country.first()(0)
  }

  /**
    * How many hotels are within 10 km (<=10000.0) from
    * given latitude and longitude?
    * https://en.wikipedia.org/wiki/Haversine_formula
    * earth radius is 6371e3 meters.
    *
    * Location is a hotel if the name contains the word 'hotel'.
    * Don't use feature code field!
    *
    * Important
    *   if you want to use helper functions, use variables as
    *   functions, e.g
    *   val distance = (a: Double) => {...}
    *
    * @param lat latitude as Double
    * @param long longitude as Double
    * @return number of hotels in area
    */
  def hotelsInArea(lat: Double, long: Double): Int = {

    // Filter the required data from the file
    val data_indexed = file.map( line => line.split('\t')).map(y => y.zipWithIndex)
    val data = data_indexed.map(arr => arr.filter {
      case (value, index) => index match {
        case 2  => true
        case 4  => true
        case 5  => true
        case _  => false
      } }).map(arr => arr.map(elem => elem._1))

    // Calculate the distance using Haversine formula
    val distanceInMeters = (lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
      val lat1_rad = math.Pi / 180.0 * lat1
      val lon1_rad = math.Pi / 180.0 * lon1
      val lat2_rad = math.Pi / 180.0 * lat2
      val lon2_rad = math.Pi / 180.0 * lon2

      val dlon_rad = lon2_rad - lon1_rad
      val dlat_rad = lat2_rad - lat1_rad
      val tmp1 = math.pow(math.sin(dlat_rad / 2), 2) + math.cos(lat1_rad) * math.cos(lat2_rad) * math.pow(math.sin(dlon_rad / 2), 2)
      val tmp2 = 2 * math.atan2(math.sqrt(tmp1), math.sqrt(1 - tmp1))
      val dist_in_meters = 6371 * tmp2 * 1000
      dist_in_meters
    }

    // Calculate the hotels in area by first filtering out the duplicates
    // and then eliminating the case issues and then searching for the word hotel
    val search_word:String = "hotel"
    val filt_hotels = data.filter(x => x(0).toLowerCase.contains(search_word))
    val hotels_dist = filt_hotels.map(x => ( x(0), distanceInMeters(x(1).toDouble, x(2).toDouble, lat, long) ))

    val hotels_within_10KM = hotels_dist.filter(x => x._2 <= 10000.0)
    val num_hotels = hotels_within_10KM.distinct().count().toInt
    num_hotels
  }

  //GraphX exercises

  /**
    * Load FourSquare social graph data, create a
    * graphx graph and return it.
    * Use user id as vertex id and vertex attribute.
    * Use number of unique connections between users as edge weight.
    * E.g
    * ---------------------
    * | user_id | dest_id |
    * ---------------------
    * |    1    |    2    |
    * |    1    |    2    |
    * |    2    |    1    |
    * |    1    |    3    |
    * |    2    |    3    |
    * ---------------------
    *         || ||
    *         || ||
    *         \   /
    *          \ /
    *           +
    *
    *         _ 3 _
    *         /' '\
    *        (1)  (1)
    *        /      \
    *       1--(2)--->2
    *        \       /
    *         \-(1)-/
    *
    * Hints:
    *  - Regex is extremely useful when parsing the data in this case.
    *  - http://spark.apache.org/docs/1.6.1/graphx-programming-guide.html
    *
    * @param path to file. You can find the dataset
    *  from the resources folder
    * @return graphx graph
    *
    */
  def loadSocial(path: String): Graph[Int,Int] = {

    val input = sc.textFile(path)
    println(input.count().toString)
    val filt_input = input.filter(line => line.contains("|"))
    println(filt_input.count().toString)
    val pattern = "[0-9]+".r
    val data = filt_input.map(x => pattern.findAllIn(x).toArray).filter(y => !y.isEmpty)
    val vert = data.flatMap(y => y).distinct
    vert.foreach(println)
    val vert_data = vert.map(x => (x.toLong, x.toInt))

    //val filt_edges = data.filter(arr => arr.length == 2)
    val edge_data = data.map(arr => Edge(arr(0).toLong, arr(1).toLong, arr(0).toInt))
    val graph = Graph(vert_data, edge_data)
    graph
  }

  /**
    * Which user has the most outward connections.
    *
    * @param graph graphx graph containing the data
    * @return vertex_id as Int
    */
  def mostActiveUser(graph: Graph[Int,Int]): Int = {

    val vert_outdeg = graph.outDegrees
    val active_user = vert_outdeg.join(graph.vertices).sortBy(_._2._1, ascending=false).first
    active_user._1.toInt
  }

  /**
    * Which user has the highest pageRank.
    * https://en.wikipedia.org/wiki/PageRank
    *
    * @param graph graphx graph containing the data
    * @return user with highest pageRank
    */
  def pageRankHighest(graph: Graph[Int,Int]): Int = {

    val rank_vertices = graph.pageRank(0.0001).vertices
    val user_high_rank = rank_vertices.join(graph.vertices).sortBy(_._2._1, ascending=false).first
    user_high_rank._1.toInt
  }
}
/**
  *
  *  Change the student id
  */
object GeoProcessor {
  val studentId = "546289"
}
