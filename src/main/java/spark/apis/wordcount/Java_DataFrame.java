package spark.apis.wordcount;

import org.apache.spark.sql.*;

public class Java_DataFrame {

    public static void main(String... args) {

        // Initialization:
        int threads = 3; // program simulates a single executor with 3 cores (one local JVM with 3 threads)
        SparkSession session = SparkSession
                .builder()
                .appName("Java WordCount DataFrame")
                .master("local[" + threads + "]")
                .getOrCreate();
        SQLContext context = session.sqlContext();
        Dataset<Row> lines = context.read().text("src/main/resources/moby_dick.txt");

        // Preprocessing & reducing the input lines:
        Dataset<Row> words = lines.select(functions.explode(functions.split(lines.col("value"), "\\s+")));
        Dataset<Row> tokens = words.where(functions.length(words.col("col")).gt(0)) // filter empty strings
                .select(functions.lower(words.col("col")).alias("token")); // lowercase strings
        Dataset<Row> counts = tokens.groupBy("token").count();

        // materializing to local disk:
        counts.coalesce(threads)  // optional, without coalesce many tiny output files are generated
                .write().csv("./countsplits");

        session.stop();

    }
}
