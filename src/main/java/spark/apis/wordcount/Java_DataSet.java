package spark.apis.wordcount;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import java.util.Arrays;

public class Java_DataSet {

    public static void main(String... args) {

        // Initialization:
        int threads = 3; // program simulates a single executor with 3 cores (one local JVM with 3 threads)

        SparkSession session = SparkSession
                .builder()
                .appName("Java WordCount Dataset")
                .master("local[" + threads + "]")
                .getOrCreate();

        // Preprocessing & reducing the input lines:
        Dataset<String> lines = session.read().text("src/main/resources/moby_dick.txt").as(Encoders.STRING());
        Dataset<String> tokens = lines.flatMap((FlatMapFunction<String, String>) text -> Arrays.asList(text.toLowerCase().split("\\s+")).iterator(), Encoders.STRING());
        Dataset<Tuple2<String, Object>> counts = tokens.filter((FilterFunction<String>) s -> !s.isEmpty()) // filter empty strings
                .groupByKey((MapFunction<String, String>)token -> token, Encoders.STRING())
                .count();

        // Materializing to local disk:
        counts.coalesce(threads) // optional, without coalesce many tiny output files are generated
                .write().csv("./countsplits");

        session.stop();
    }
}
