package com.Talentica.SparkDemo1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class DataTransform {

	public static final String INPUT_PATH = "oc-stats-analytics.csv";
//	public static final String INPUT_PATH = "empty.csv";
	public static final String OUTPUT_PATH = "output.csv";
	public static final String DELIMITER = ",";
	static String[] partitionNames = null;
	static String[] partitionNamesModified = null;

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("CSVDataManipulation");
		JavaSparkContext sc = new JavaSparkContext(conf);

		File file = new File(INPUT_PATH);
		
		
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String header = reader.readLine();
			if(header.length()==0) {
				sc.close();
				System.out.println("Input_file is empty");
				return;
			}
			partitionNames = header.split(DELIMITER);
			partitionNamesModified = new String[partitionNames.length - 1];
			for (int i = 1; i < partitionNames.length; i++) {
				String[] m = partitionNames[i].split(":")[1].split("\\.");
				partitionNamesModified[i - 1] = m[5] + "_" + m[3];
			}
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());;
			sc.close();
			return;
		} catch (IOException e) {
			System.out.println(e.getMessage());;
			sc.close();
			return;
		}
		
		sc.textFile(INPUT_PATH).filter(line -> !line.contains(":")).flatMapToPair((String x) -> {
			String[] line = x.split(",");
			String timestamp = line[0];
			return IntStream.range(1, line.length).mapToObj((i) -> {
				try {
					return new Tuple2<String, Tuple2<String, Double>>(partitionNamesModified[i - 1],
							new Tuple2<String, Double>(timestamp, Double.parseDouble(line[i])));
				} catch (Exception e) {
					return null;
				}
			}).filter(tuple -> tuple != null).collect(Collectors.toList()).iterator();
		}).sortByKey().map((tuple) -> {
			return String.format("%s,%s,%s", tuple._2()._1(), tuple._1(), tuple._2()._2());
		}).saveAsTextFile("output");
		
		new File("output/part-00000").renameTo(new File("output.csv"));

		sc.close();
	}
}
