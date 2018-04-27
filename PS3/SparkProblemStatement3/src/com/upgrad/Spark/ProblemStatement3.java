package com.upgrad.Spark;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

public class ProblemStatement3 {

	public static class YelloTaxiTrip {

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

	@SuppressWarnings("serial")
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

	@SuppressWarnings("serial")
	public static class YtToString implements FlatMapFunction<Iterator<YelloTaxiTrip>, String> {
		public Iterator<String> call(Iterator<YelloTaxiTrip> ytps) throws Exception {
			ArrayList<String> tripData = new ArrayList<String>();

			while (ytps.hasNext()) {
				YelloTaxiTrip ytp = ytps.next();
				try {
					StringBuilder s = new StringBuilder();
					if (ytp.equals(null)) {
						s = s.append("");
					} else {
						s = s.append(ytp.VendorID).append(",").append(ytp.tpep_pickup_datetime).append(",")
								.append(ytp.tpep_dropoff_datetime).append(",").append(ytp.passenger_count).append(",")
								.append(ytp.trip_distance).append(",").append(ytp.RatecodeID).append(",")
								.append(ytp.store_and_fwd_flag).append(",").append(ytp.PULocationID).append(",")
								.append(ytp.DOLocationID).append(",").append(ytp.payment_type).append(",")
								.append(ytp.fare_amount).append(",").append(ytp.extra).append(",").append(ytp.mta_tax)
								.append(",").append(ytp.tip_amount).append(",").append(ytp.tolls_amount).append(",")
								.append(ytp.improvement_surcharge).append(",").append(ytp.total_amount);

					}

					tripData.add(s.toString());

				} catch (Exception e) {
					System.out.println(" Received Exception while parsing the csvFile. Exception is: " + e.toString());
					e.printStackTrace();
				}
			}

			return tripData.iterator();

		}
	}

	@SuppressWarnings("serial")
	public static class Filter2 implements Function<YelloTaxiTrip, Boolean> {
		public Boolean call(YelloTaxiTrip ytp) {

			return ytp.RatecodeID.equals("4") ? true : false;

		}
	}

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\winutil");

		// Logging only errors
		Logger.getLogger("org").setLevel(Level.ERROR);

		// configure spark
		SparkConf conf = new SparkConf().setAppName("ProblemStatment2").setMaster("local[*]");
		// start a spark context
		JavaSparkContext sc = new JavaSparkContext(conf);

		// read text file to RDD
		JavaRDD<String> lines = sc.textFile("in/trip_yellow_taxi.data");

		// Converting the String to a class type and filtering based on the filter data
		JavaRDD<YelloTaxiTrip> result = lines.mapPartitions(new ParseCsv()).filter(new Filter2());

		// Converting back to String as its required while saving the file as text
		// content.
		JavaRDD<String> stringResult = result.mapPartitions(new YtToString());

		// Saving the RDD as text file.
		stringResult.saveAsTextFile("PS2Output");

	}

	public static String yeptoString(YelloTaxiTrip ytp) {
		StringBuilder s = new StringBuilder();
		if (ytp.equals(null)) {
			s = s.append("");
		} else {
			s = s.append(ytp.VendorID).append(",").append(ytp.tpep_pickup_datetime).append(",")
					.append(ytp.tpep_dropoff_datetime).append(",").append(ytp.passenger_count).append(",")
					.append(ytp.trip_distance).append(",").append(ytp.RatecodeID).append(",")
					.append(ytp.store_and_fwd_flag).append(",").append(ytp.PULocationID).append(",")
					.append(ytp.DOLocationID).append(",").append(ytp.payment_type).append(",").append(ytp.fare_amount)
					.append(",").append(ytp.extra).append(",").append(ytp.mta_tax).append(",").append(ytp.tip_amount)
					.append(",").append(ytp.tolls_amount).append(",").append(ytp.improvement_surcharge).append(",")
					.append(ytp.total_amount);

		}
		return s.toString();
	}

}








/*package com.upgrad.Spark;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

//import scala.Tuple1;
//import scala.Tuple10;
//import scala.Tuple2;


public class ProblemStatement3 {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\winutil");

		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkSession session = SparkSession.builder().appName("ProblemStatement3").master("local[*]").getOrCreate();		
		DataFrameReader dataframeReader = session.read();
		
		Dataset<Row> ratings = dataframeReader.option("header","true").csv("in/trip_yellow_taxi.data");		
		Dataset<Row> ratingsWithoutHeader = ratings.filter("VendorID == 1 or VendorID == 2");
		Dataset<Row> groupedData = ratingsWithoutHeader.groupBy("payment_type").count();
		Dataset<Row> sortedData = groupedData.orderBy(groupedData.col("count").asc());
		
		sortedData.rdd().saveAsTextFile("ProblemStatement3");
		
	}

}
*/