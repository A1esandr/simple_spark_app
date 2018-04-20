import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.datediff;

public class SimpleApp {
    public static void main(String[] args) {

        if(args.length == 0){
            throw new IllegalArgumentException("No arguments supplied!");
        }

        String mode;
        String logFile = "";
        String appLoadedFile = "events/app_loaded/*.parquet";
        String registeredFile = "events/registered/*.parquet";

        switch (args[0].charAt(0)) {
            case '-':
                if (args[0].length() < 2)
                    throw new IllegalArgumentException("Not a valid argument: "+args[0]);

                if(args[0].equals("-file")){
                    mode = "file";
                    logFile = args[1];
                    if(args.length > 2)
                        appLoadedFile = args[2];
                    if(args.length > 3)
                        registeredFile = args[3];
                } else if(args[0].equals("-compute")){
                    mode = "compute";
                    if(args.length > 1)
                        appLoadedFile = args[1];
                    if(args.length > 2)
                        registeredFile = args[2];
                } else {
                    throw new IllegalArgumentException("Not supposed argument: "+args[0]);
                }

                break;
            default:
                throw new IllegalArgumentException("Only arguments with preceding dashes supposed!");
        }

        SparkSession spark = SparkSession.builder().appName("Simple Spark Application").getOrCreate();

        if(mode.equals("file")){
            Dataset<Row> df = spark.read().json(logFile);

            Dataset<Row> df3 = df.filter(col("_n").equalTo("app_loaded"))
                    .withColumn("timeTmp", col("_t").cast(DataTypes.TimestampType))
                    .drop("_t")
                    .withColumnRenamed("timeTmp", "_t")
                    .select("_t", "_p", "device_type")
                    .toDF("time","email","device_type");

            df3.write().mode("Overwrite").parquet(appLoadedFile);

            Dataset<Row> df4 = df.filter(col("_n").equalTo("registered"))
                    .withColumn("timeTmp", col("_t").cast(DataTypes.TimestampType))
                    .drop("_t")
                    .withColumnRenamed("timeTmp", "_t")
                    .select("_t", "_p", "channel")
                    .toDF("time","email","channel");

            df4.write().mode("Overwrite").parquet(registeredFile);

            System.out.println("Data successfully loaded!");
        } else if(mode.equals("compute")){
            Dataset<Row> df3 = spark.read().parquet(appLoadedFile);
            Dataset<Row> df4 = spark.read().parquet(registeredFile);

            float c0 = df4.groupBy(df4.col("email")).count().toDF().count();

            float c1 = df4.join(df3, df4.col("email").equalTo(df3.col("email")))
                    .filter(datediff(df3.col("time"), df4.col("time")).lt(7))
                    .groupBy(df4.col("email"))
                    .count().toDF().count();

            float c2 = c0 > 0 ? (c1 / c0 * 100): 0;

            System.out.println("Total number of registered users: " + (int)c0);
            System.out.println("Total number of users who downloaded the app within a week after registration: " + (int)c1);

            System.out.println("Percent of users who downloaded the app within a week after registration: " + c2);
        }

        spark.stop();
    }
}
