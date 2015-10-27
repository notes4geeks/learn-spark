package com.notes4geeks.learn.spark;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class SparkEvenByFilter {

	public static void main(String[] args) {
		String master = args.length > 0 ? args[0] : "local[4]";
		
		Integer[] arr = new Integer[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
		
		JavaSparkContext sc = new JavaSparkContext(master, SparkSumByFold.class.getSimpleName());
		
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(arr));
		
		JavaRDD<Integer> evenNos = rdd.filter(new Function<Integer, Boolean>() {
			
			@Override
			public Boolean call(Integer v1) throws Exception {
				return v1%2 == 0;
			}
		});
		
		// print the even nos out of all Nos.
		evenNos.foreach(new VoidFunction<Integer>() {
			
			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
				
			}
		});
		
		sc.close();

	}

}
