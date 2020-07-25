package com;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("Spark application")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", 5)
                .getOrCreate();

        String path = new App().getClass().getClassLoader().getResource("data/csv/2015-summary.csv").getPath();

        Dataset<Row> flightData2015 = sparkSession
                .read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv(path);

        // Taking 3 rows from the flight dataset
       /* Object [] dataObjects = (Object[]) flightData2015.take(3);
        for(Object object: dataObjects) {
            System.out.println(object);
        }*/

        // Explain the physical plan
        //flightData2015.sort("count").take(2);
        RelationalGroupedDataset dest_country_name = flightData2015.groupBy("DEST_COUNTRY_NAME");
        Dataset<Row> count = dest_country_name.sum("count");
        Dataset<Row> destination_total = count.withColumnRenamed("sum(count)", "destination_total");
        Dataset<Row> sort = ((Dataset<Row>) destination_total).sort("desc", "destination_total");
        Object collect = sort.limit(5).collect();
        System.out.println();

    }

}
