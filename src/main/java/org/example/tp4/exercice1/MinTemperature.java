package org.example.tp4.exercice1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import static jersey.repackaged.com.google.common.base.Preconditions.checkArgument;

public class MinTemperature {
    public static void main(String[] args) {
        checkArgument(args.length > 1, "Please provide the path of input file and output dir as parameters.");
        new MinTemperature().calculateMinTemperature(args[0], args[1]);
    }

    public void calculateMinTemperature(String inputFilePath, String outputDir){
        String master = "local[*]";
        SparkConf conf = new SparkConf()
                .setAppName(MinTemperature.class.getName())
                .setMaster(master);

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(inputFilePath);

        JavaPairRDD<String, Integer> maxTemperature = lines.mapToPair(line -> {
            String year = line.substring(15, 19);
            String temperatureStr = line.charAt(40) == '+' ? line.substring(41, 45) : line.substring(40, 45);
            int airTemperature = Integer.parseInt(temperatureStr);
            return new Tuple2<>(year, airTemperature);
        }).reduceByKey(Math::min);

        maxTemperature.saveAsTextFile(outputDir);
    }
}

