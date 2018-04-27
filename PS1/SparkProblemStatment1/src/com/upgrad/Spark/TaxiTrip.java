package com.upgrad.Spark;
	

import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.lang.Iterable;
import scala.Tuple2;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class TaxiTrip implements Serializable{
	
	public  int VendorID ;
	public  String tpep_pickup_datetime ;
	public  String tpep_dropoff_datetime ;
	public  int passenger_count ;
	public  float trip_distance; 
	public  int RatecodeID;
	public  String store_and_fwd_flag;
	public  String PULocationID; 
	public  String DOLocationID; 
	public  int payment_type; 
	public  float fare_amount;
	public  float extra;
	public  float mta_tax;
	public  float tip_amount;
	public  float tolls_amount;
	public  float improvement_surcharge;
	public  float total_amount;

}
