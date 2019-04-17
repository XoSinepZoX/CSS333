import scala.Tuple2;
import java.util.List;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;

public class ex4{
  public static void main(String[] args){
    SparkSession spark = SparkSession.builder().getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    JavaRDD<String> lines = jsc.textFile("/home/student/css333/dataset/accesslog.csv");
    JavaPairRDD<String, Integer> ips = lines.mapToPair( str -> {String[] cols = str.split(","); return new Tuple2<String, Integer>(cols[7],1); });
    JavaPairRDD<String, Integer> counts = ips.reduceByKey( (i,j) -> i+j );
    JavaPairRDD<String,Integer> sorted = counts.mapToPair(t -> t.swap()).sortByKey(false).mapToPair(t -> t.swap());

    List<Tuple2<String, Integer>> output = sorted.collect();
    for(Tuple2<?,?> tuple : output){
        System.out.println(tuple._1() + ": " + tuple._2());
    }

    spark.stop();
  }
}


// javac -cp "/opt/spark-2.3.3-bin-hadoop2.7/jars/*" ex4.java
// jar -cvf ex4.jar ex4.class
// spark-submit --master local[2] --class ex4 ex4.jar | more
