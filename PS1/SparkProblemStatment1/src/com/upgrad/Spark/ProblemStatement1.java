package com.upgrad.Spark;

import java.lang.Integer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.catalyst.parser.ParserInterface;

import scala.Tuple6;





public class ProblemStatement1 {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\winutil");
		
		//Logging only errors
		//Logger.getLogger("org").setLevel(Level.ERROR);

		// configure spark
		SparkConf conf = new SparkConf().setAppName("ProblemStatment1").setMaster("local[*]");
		// start a spark context
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// read text file to RDD
		JavaRDD<String> lines = sc.textFile("in/trip_yellow_taxi.data");
		
		System.out.println(lines.count());
		
		/*JavaRDD<Tuple6<Integer, String, String, Integer, Float, String >> data = 
				lines.map(x -> x.split(",")).
				map(x -> new Tuple6<>(parseInt(x[0]), x[1], x[2], parseInt(x[3]), parseFloat(x[4]), x[5]));*/
		
		//lines.foreach(x -> System.out.println(x.split(",")[4] + " :: " + x.split(",")[1]));
		
		/*JavaRDD<String> filteredLines = lines.filter(x -> x.contains("2017-10-01 00:15:30"));
		JavaRDD<String> filteredLines1 = lines.filter(new Function<String, Boolean>() {
			public Boolean call(String s) throws Exception {
			if(s == null || s.trim().length() < 1) {
				return false;
			} 
			else 
				if (s.split(",")[0] == "2") 
				{ 
					return true;
				} else 
				{
					return false;
				}
			}
			});
		System.out.println(filteredLines1.count());
				//filter(x -> x.split(",")[1]  == "2017-10-01 00:15:30");/*.
				filter(x -> x.split(",")[2]  == "2017-10-01 00:25:11").
				filter(x -> x.split(",")[3]  == "1").
				filter(x -> x.split(",")[4]  == "2.17")
				 
		filteredLines.foreach(x -> System.out.println(x));
		System.out.println(filteredLines.count());*/
		
		//&&  && x.split(",")[3]  == "1" && x.split(",")[4]  == "2.17" 
		// VendorID==2 AND 
		/*tpep_pickup_datetime=='2017-10-01 00:15:30' AND 
				tpep_dropoff_datetime=='2017-10-01 00:25:11' AND 
				passenger_count==1 AND trip_distance==2.17");
*/		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
//		SparkSession session = SparkSession.builder().appName("ProblemStatement1").master("local[*]").getOrCreate();		
/*		DataFrameReader dataframeReader = session.read();
		
		Dataset<Row> ratings = dataframeReader.option("header","true").csv("in/trip_yellow_taxi.data");		
		Dataset<Row> ratingsWithoutHeader = ratings.filter("VendorID == 1 or VendorID == 2");		
		Dataset<Row> filteredRatings = ratingsWithoutHeader.filter("VendorID==2 AND tpep_pickup_datetime=='2017-10-01 00:15:30' AND tpep_dropoff_datetime=='2017-10-01 00:25:11' AND passenger_count==1 AND trip_distance==2.17");
		
		filteredRatings.rdd().saveAsTextFile("ProblemStatememnt1");*/

	}

}
