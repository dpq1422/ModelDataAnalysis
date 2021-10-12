package com.dpq.model.data.driver;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;



public class ModelDataAnalysis {

	
	public static void main(String[] args) throws InterruptedException {
		 JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"));
		 SparkSession spark = SparkSession.builder().appName("spark-bigquery-demo").getOrCreate();
		 Dataset<Row> row = spark.read().csv("/Users/dpq/springbootWrokspace/CountryDataAnalysis/resources/modeloutput.csv");
		 // way 1 to change column name
		 row = row.withColumnRenamed("_c0", "CountryName");
		 row = row.withColumnRenamed("_c1", "ReportingPurpuse");
		 row = row.withColumnRenamed("_c2", "Quarter");
		 row = row.withColumnRenamed("_c3", "Grain");
		 row = row.withColumnRenamed("_c4", "PP0");
		 row = row.withColumnRenamed("_c5", "PP1");
		 row = row.persist();
		 row.show();
		 Arrays.stream(row.columns()).forEach(column -> System.out.println(column));
		 
		 /**Seq<String> sourceColumns = JavaConverters.asScalaBuffer( Arrays.asList(row.columns()) );
		 Column [] arr = {new Column("COUNTRY"),new Column("REPORTING_PERIOD"),
				 new Column("QUATER"), new Column("GRAIN"), new Column("PROJECTED_1ST_MONTH"), 
				 new Column("PROJECTED_2ND_MONTH")};
		 
		 Seq<Column> destinationColumns = JavaConverters.asScalaBuffer( Arrays.asList( arr) );
		 
		 // way 2 to change ALL column name
		 row = row.withColumn(sourceColumns , destinationColumns); **/
		 row.dropDuplicates().show();
		 System.out.println(row.count());
		 System.out.println(row.dropDuplicates().count());
		    
		 //removing duplicates
		 row = row.dropDuplicates();
		 //creating table Model_Output on top of file which loaded
		 row.createOrReplaceTempView("Model_Output");
		 // way 2 to change column nameS
		 Dataset<Row> sqlDF = spark.sql("SELECT CountryName as COUNTRY, ReportingPurpuse AS RPT_PRD, "
		 		+ "Quarter AS QUATER, Grain AS GRAIN, PP0 AS PRE_PROJECTED_1ST_MONTH,PP1 AS PRE_PROJECTED_2ND_MONTH FROM Model_Output");
		//1ST way to filter records
		 Dataset<Row> filteredRecords1=sqlDF.filter(sqlDF.col("QUATER").isInCollection(Arrays.asList("2017Q1", "2018Q1", "2019Q1")));
		 filteredRecords1.show();
		//2nd way to filter records
		 sqlDF = spark.sql("SELECT CountryName as COUNTRY, ReportingPurpuse AS RPT_PRD, "
			 		+ "Quarter AS QUATER, Grain AS GRAIN, PP0 AS PRE_PROJECTED_1ST_MONTH,PP1 AS PRE_PROJECTED_2ND_MONTH "
			 		+ "FROM Model_Output where Quarter not in (\"2016Q1\")");
		 sqlDF.show();
		 
		 Dataset<Row> finalAggregatedResults = spark.sql("SELECT CountryName as COUNTRY, ReportingPurpuse AS RPT_PRD, "
			 		+ "Quarter AS QUATER, Grain AS GRAIN, sum(CAST(PP0 AS DOUBLE)) ,sum(CAST(PP1 AS DOUBLE))  "
			 		+ "FROM Model_Output group by CountryName,ReportingPurpuse,Quarter,Grain");
		 finalAggregatedResults.show();
		 
				
		sc.close();
	}
	
	
	
	
}

