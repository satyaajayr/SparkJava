package spark.testproject;

/*import java.nio.file.Path;
import java.nio.file.Paths;*/

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ProblemStatement2 {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		//Setting up path for the Winutil
		//Path path = Paths.get("in", "Winutil");
		//System.setProperty("hadoop.home.dir", path.toAbsolutePath().toString());

		// configure spark
		SparkConf conf = new SparkConf().setAppName("ProblemStatement2").setMaster("local[*]");
		// start a spark context
		JavaSparkContext sc = new JavaSparkContext(conf);

		// read text file to RDD
		JavaRDD<String> lines = sc.textFile(args[0]);

		// Filtering the lines based on the Problem conditions
		JavaRDD<String> filteredLines = lines.filter(x -> !x.split(",")[0].equals("VendorID") && !x.isEmpty())
				.filter(x -> {
					String[] cols = x.split(",");
					return cols[5].equals("4") ? true : false;
				});

		// Saving the output in a Text file.
		filteredLines.saveAsTextFile(args[1]);

	}

}

/*package com.upgrad.Spark;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

public class ProblemStatement2 {

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

//
// result.take(5).forEach(x -> System.out.println(x.passenger_count));

 * public int VendorID ; public String tpep_pickup_datetime ; public String
 * tpep_dropoff_datetime ; public int passenger_count ; public float
 * trip_distance; public int RatecodeID; public String store_and_fwd_flag;
 * public String PULocationID; public String DOLocationID; public int
 * payment_type; public float fare_amount; public float extra; public float
 * mta_tax; public float tip_amount; public float tolls_amount; public float
 * improvement_surcharge; public float total_amount;
 

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
 

 * JavaRDD<Tuple6<Integer, String, String, Integer, Float, String >> data =
 * lines.map(x -> x.split(",")). map(x -> new Tuple6<>(parseInt(x[0]), x[1],
 * x[2], parseInt(x[3]), parseFloat(x[4]), x[5]));
 

// lines.foreach(x -> System.out.println(x.split(",")[4] + " :: " +
// x.split(",")[1]));


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
 

// && && x.split(",")[3] == "1" && x.split(",")[4] == "2.17"
// VendorID==2 AND

 * tpep_pickup_datetime=='2017-10-01 00:15:30' AND
 * tpep_dropoff_datetime=='2017-10-01 00:25:11' AND passenger_count==1 AND
 * trip_distance==2.17");
 

// SparkSession session =
// SparkSession.builder().appName("ProblemStatement1").master("local[*]").getOrCreate();

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
 


 * package com.upgrad.Spark;
 * 
 * import java.util.Arrays;
 * 
 * import org.apache.log4j.Level; import org.apache.log4j.Logger; import
 * org.apache.spark.SparkConf; import org.apache.spark.api.java.JavaPairRDD;
 * import org.apache.spark.api.java.JavaRDD; import
 * org.apache.spark.api.java.JavaSparkContext; import
 * org.apache.spark.api.java.function.Function;
 * 
 * //import scala.Tuple1; //import scala.Tuple10; //import scala.Tuple2;
 * 
 * 
 * public class ProblemStatement2 {
 * 
 * public static void main(String[] args) {
 * System.setProperty("hadoop.home.dir", "C:\\winutil");
 * 
 * Logger.getLogger("org").setLevel(Level.ERROR); SparkSession session =
 * SparkSession.builder().appName("ProblemStatement2").master("local[*]").
 * getOrCreate(); DataFrameReader dataframeReader = session.read();
 * 
 * Dataset<Row> ratings =
 * dataframeReader.option("header","true").csv("in/trip_yellow_taxi.data");
 * Dataset<Row> ratingsWithoutHeader =
 * ratings.filter("VendorID == 1 or VendorID == 2"); Dataset<Row>
 * filteredRatings = ratingsWithoutHeader.filter("RatecodeID==4");
 * 
 * filteredRatings.rdd().saveAsTextFile("ProblemStatement2");
 * 
 * }
 * 
 * }
 */