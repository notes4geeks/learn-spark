package com.notes4geeks.learn.spark.basic;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class SparkNosByIntersection {

	public static void main(String[] args) {
String master = args.length > 0 ? args[0] : "local[4]";
		
		JavaSparkContext sc = new JavaSparkContext(master, SparkSumByFold.class.getSimpleName());
		
		JavaRDD<Integer> oddNos = sc.parallelize(Arrays.asList(1, 3, 5, 3, 7, 7, 9, 1));
		JavaRDD<Integer> evenNos = sc.parallelize(Arrays.asList(2, 4, 2, 2, 2, 6, 0, 4));
		 
		JavaRDD<Integer> subtractedNos = oddNos.subtract(evenNos);
		
		System.out.println("printing nos which are in odd but not in even, will print all odd nos.");
		
		// print the nos.
		subtractedNos.foreach(new VoidFunction<Integer>() {
			
			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
				
			}
		});
		
		sc.close();

	}

}
