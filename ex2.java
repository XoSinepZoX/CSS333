import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class ex2{
  public static void main(String[] args){
    SparkSession spark = SparkSession.builder().getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    JavaRDD<String> dataRdd = jsc.textFile("/home/student/css333/dataset/randoms.txt");
    JavaRDD<Integer> newRdd = dataRdd.map(str -> Integer.parseInt(str));

    JavaRDD<Integer> sortRdd = newRdd.sortBy(i -> i, true, 1);

    System.out.println("Saving...");
    sortRdd.saveAsTextFile("/home/student/css333/example/sorted");
    spark.stop();
  }
}


// javac -cp "/opt/spark-2.3.3-bin-hadoop2.7/jars/*" ex2.java
// jar -cvf ex2.jar ex2.class
// spark-submit --master local[2] --class ex2 ex2.jar
