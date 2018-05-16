package spark.apis.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;

public class Java_RDD {

    public static void main(String... args) {

        // Initialization:
        int threads = 3; // program simulates a single executor with 3 cores (one local JVM with 3 threads)
        SparkConf conf = new SparkConf()
                .setMaster("local[" + threads + "]")
                .setAppName("Java WordCount RDD");
        JavaSparkContext context = new JavaSparkContext(conf);

        // Preprocessing & reducing the input lines:
        JavaRDD<String> lines = context.textFile("src/main/resources/moby_dick.txt");
        JavaRDD<String> words = lines.flatMap(text -> Arrays.asList(text.split("\\s+")).iterator());
        JavaPairRDD<String, Integer> tokenFrequ = words.mapToPair(word -> new Tuple2<>(word.toLowerCase(), 1));
        JavaPairRDD<String, Integer> counts = tokenFrequ.reduceByKey((a, b) -> a + b);

        // Materializing to local disk:
        counts.coalesce(threads) // optional, without coalesce many tiny output files are generated
                .saveAsTextFile("./countsplits");

        context.stop();
    }
}
