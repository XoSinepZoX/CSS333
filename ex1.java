import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class ex1{
  public static void main(String[] args){
    SparkSession spark = SparkSession.builder().getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    JavaRDD<String> dataRdd = jsc.textFile("/home/student/css333/dataset/randoms.txt");
    JavaRDD<Integer> newRdd = dataRdd.map(str -> Integer.parseInt(str));

    int max = newRdd.reduce( (i,j) -> {
      if (i > j) {
        return i;
      } else {
        return j;
      }
    });

    int min = newRdd.reduce( (i,j) -> {
      if (i < j) {
        return i;
      } else {
        return j;
      }
    });

    System.out.println("Maximum: "+max);
    System.out.println("Minimum: "+min);
    spark.stop();
  }
}


// javac -cp "/opt/spark-2.3.3-bin-hadoop2.7/jars/*" ex1.java
// jar -cvf ex1.jar ex1.class
// spark-submit --master local[2] --class ex1 ex1.jar
