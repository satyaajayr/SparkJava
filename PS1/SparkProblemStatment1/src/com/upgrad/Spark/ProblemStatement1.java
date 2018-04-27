package com.upgrad.Spark;

import java.io.Serializable;
import java.lang.Integer;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.spark_project.jetty.util.HttpCookieStore.Empty;

import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple6;

public class ProblemStatement1 {

	public static class YelloTaxiTrip implements java.io.Serializable {

		public String VendorID;
		public String tpep_pickup_datetime;
		public String tpep_dropoff_datetime;
		public String passenger_count;
		public String trip_distance;
		public String RatecodeID;
		public String store_and_fwd_flag;
		public String PULocationID;
		public String DOLocationID;
		public String payment_type;
		public String fare_amount;
		public String extra;
		public String mta_tax;
		public String tip_amount;
		public String tolls_amount;
		public String improvement_surcharge;
		public String total_amount;

	}

	public static class ParseCsv implements FlatMapFunction<Iterator<String>, YelloTaxiTrip> {
		public Iterator<YelloTaxiTrip> call(Iterator<String> lines) throws Exception {
			ArrayList<YelloTaxiTrip> tripData = new ArrayList<YelloTaxiTrip>();

			while (lines.hasNext()) {
				String line = lines.next();
				try {
					String[] cols = line.split(",");

					YelloTaxiTrip ytp = new YelloTaxiTrip();

					if (!cols[0].equals("VendorID") && !line.isEmpty()) {
						ytp.VendorID = cols[0];
						ytp.tpep_pickup_datetime = cols[1];
						ytp.tpep_dropoff_datetime = cols[2];
						ytp.passenger_count = cols[3];
						ytp.trip_distance = cols[4];
						ytp.RatecodeID = cols[5];
						ytp.store_and_fwd_flag = cols[6];
						ytp.PULocationID = cols[7];
						ytp.DOLocationID = cols[8];
						ytp.payment_type = cols[9];
						ytp.fare_amount = cols[10];
						ytp.mta_tax = cols[11];
						ytp.tip_amount = cols[12];
						ytp.tolls_amount = cols[13];
						ytp.improvement_surcharge = cols[14];
						ytp.total_amount = cols[15];

						tripData.add(ytp);

					}
				} catch (Exception e) {
					System.out.println(" Received Exception while parsing the csvFile. Exception is: " + e.toString());
					e.printStackTrace();
				}
			}

			return tripData.iterator();

		}
	}

	public static class Filter1 implements Function<YelloTaxiTrip, Boolean> {
		public Boolean call(YelloTaxiTrip ytp) {

			return ytp.VendorID.equals("2") && ytp.tpep_pickup_datetime.equals("2017-10-01 00:15:30")
					&& ytp.tpep_dropoff_datetime.equals("2017-10-01 00:25:11") && ytp.passenger_count.equals("1")
					&& ytp.trip_distance.equals("2.17") ? true : false;

		}
	}

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\winutil");

		// Logging only errors
		Logger.getLogger("org").setLevel(Level.ERROR);

		// configure spark
		SparkConf conf = new SparkConf().setAppName("ProblemStatment1").setMaster("local[*]");
		// start a spark context
		JavaSparkContext sc = new JavaSparkContext(conf);

		// read text file to RDD
		JavaRDD<String> lines = sc.textFile("in/trip_yellow_taxi.data");

		JavaRDD<YelloTaxiTrip> result = lines.mapPartitions(new ParseCsv()).filter(new Filter1());
		
		result.saveAsTextFile("PS1Output");

		//result.foreach(x -> System.out.println(yeptoString(x)));

	}

	public static String yeptoString(YelloTaxiTrip yep) {
		StringBuilder s = new StringBuilder();
		if (yep.equals(null)) {
			s = s.append("");
		} else {
			s = s.append(yep.VendorID).append(",").append(yep.tpep_pickup_datetime).append(",")
					.append(yep.tpep_dropoff_datetime).append(",").append(yep.passenger_count).append(",")
					.append(yep.trip_distance).append(",").append(yep.RatecodeID).append(",")
					.append(yep.store_and_fwd_flag).append(",").append(yep.PULocationID).append(",")
					.append(yep.DOLocationID).append(",").append(yep.payment_type).append(",").append(yep.fare_amount)
					.append(",").append(yep.extra).append(",").append(yep.mta_tax).append(",").append(yep.tip_amount)
					.append(",").append(yep.tolls_amount).append(",").append(yep.improvement_surcharge).append(",")
					.append(yep.total_amount);

		}
		return s.toString();
	}

}

//
// result.take(5).forEach(x -> System.out.println(x.passenger_count));
/*
 * public int VendorID ; public String tpep_pickup_datetime ; public String
 * tpep_dropoff_datetime ; public int passenger_count ; public float
 * trip_distance; public int RatecodeID; public String store_and_fwd_flag;
 * public String PULocationID; public String DOLocationID; public int
 * payment_type; public float fare_amount; public float extra; public float
 * mta_tax; public float tip_amount; public float tolls_amount; public float
 * improvement_surcharge; public float total_amount;
 */
/*
 * ytp.VendorID = (cols != null && cols[0] != "VendorID" && cols[0] != "") ?
 * Integer.parseInt(cols[0]) : 0 ; ytp.tpep_pickup_datetime = cols[1];
 * ytp.tpep_dropoff_datetime = cols[2]; ytp.passenger_count = (cols != null &&
 * cols[0] != "passenger_count") ? Integer.parseInt(cols[3]) : 0;
 * ytp.trip_distance = (cols != null && cols[0] != "trip_distance") ?
 * Float.parseFloat(cols[4]) : 0; ytp.RatecodeID = (cols != null && cols[0] !=
 * "RatecodeID") ? Integer.parseInt(cols[5]) : 0; ytp.store_and_fwd_flag =
 * cols[6]; ytp.PULocationID = cols[7]; ytp.DOLocationID = cols[8];
 * ytp.payment_type = (cols != null && cols[0] != "payment_type") ?
 * Integer.parseInt(cols[9]) : 0; ytp.fare_amount = (cols != null && cols[0] !=
 * "fare_amount") ? Float.parseFloat(cols[10]) : 0; ytp.mta_tax = (cols != null
 * && cols[0] != "mta_tax") ? Float.parseFloat(cols[11]) : 0; ytp.tip_amount =
 * (cols != null && cols[0] != "tip_amount") ? Float.parseFloat(cols[12]) : 0;
 * ytp.tolls_amount = (cols != null && cols[0] != "tolls_amount") ?
 * Float.parseFloat(cols[13]) : 0; ytp.improvement_surcharge = (cols != null &&
 * cols[0] != "improvement_surcharge") ? Float.parseFloat(cols[14]) : 0;
 * ytp.total_amount = (cols != null && cols[0] != "total_amount") ?
 * Float.parseFloat(cols[15]) : 0;
 */
/*
 * JavaRDD<Tuple6<Integer, String, String, Integer, Float, String >> data =
 * lines.map(x -> x.split(",")). map(x -> new Tuple6<>(parseInt(x[0]), x[1],
 * x[2], parseInt(x[3]), parseFloat(x[4]), x[5]));
 */

// lines.foreach(x -> System.out.println(x.split(",")[4] + " :: " +
// x.split(",")[1]));

/*
 * JavaRDD<String> filteredLines = lines.filter(x ->
 * x.contains("2017-10-01 00:15:30")); JavaRDD<String> filteredLines1 =
 * lines.filter(new Function<String, Boolean>() { public Boolean call(String s)
 * throws Exception { if(s == null || s.trim().length() < 1) { return false; }
 * else if (s.split(",")[0] == "2") { return true; } else { return false; } }
 * }); System.out.println(filteredLines1.count()); //filter(x -> x.split(",")[1]
 * == "2017-10-01 00:15:30");/*. filter(x -> x.split(",")[2] ==
 * "2017-10-01 00:25:11"). filter(x -> x.split(",")[3] == "1"). filter(x ->
 * x.split(",")[4] == "2.17")
 * 
 * filteredLines.foreach(x -> System.out.println(x));
 * System.out.println(filteredLines.count());
 */

// && && x.split(",")[3] == "1" && x.split(",")[4] == "2.17"
// VendorID==2 AND
/*
 * tpep_pickup_datetime=='2017-10-01 00:15:30' AND
 * tpep_dropoff_datetime=='2017-10-01 00:25:11' AND passenger_count==1 AND
 * trip_distance==2.17");
 */

// SparkSession session =
// SparkSession.builder().appName("ProblemStatement1").master("local[*]").getOrCreate();
/*
 * DataFrameReader dataframeReader = session.read();
 * 
 * Dataset<Row> ratings =
 * dataframeReader.option("header","true").csv("in/trip_yellow_taxi.data");
 * Dataset<Row> ratingsWithoutHeader =
 * ratings.filter("VendorID == 1 or VendorID == 2"); Dataset<Row>
 * filteredRatings = ratingsWithoutHeader.
 * filter("VendorID==2 AND tpep_pickup_datetime=='2017-10-01 00:15:30' AND tpep_dropoff_datetime=='2017-10-01 00:25:11' AND passenger_count==1 AND trip_distance==2.17"
 * );
 * 
 * filteredRatings.rdd().saveAsTextFile("ProblemStatememnt1");
 */
