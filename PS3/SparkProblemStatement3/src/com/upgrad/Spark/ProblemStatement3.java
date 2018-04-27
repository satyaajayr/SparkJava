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