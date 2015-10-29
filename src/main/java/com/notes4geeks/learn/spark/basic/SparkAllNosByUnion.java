package com.notes4geeks.learn.spark.basic;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class SparkAllNosByUnion {

	public static void main(String[] args) {
		String master = args.length > 0 ? args[0] : "local[4]";
		
		JavaSparkContext sc = new JavaSparkContext(master, SparkSumByFold.class.getSimpleName());
		
		JavaRDD<Integer> oddNos = sc.parallelize(Arrays.asList(1, 3, 5, 3, 7, 7, 9, 1));
		JavaRDD<Integer> evenNos = sc.parallelize(Arrays.asList(2, 4, 2, 2, 2, 6, 0, 4));
		 
		JavaRDD<Integer> allNos = oddNos.union(evenNos);
		
		System.out.println("printing all nos.");
		
		// print the all nos.
		allNos.foreach(new VoidFunction<Integer>() {
			
			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
				
			}
		});
		
		sc.close();
		
	}

}
