package com.notes4geeks.learn.spark;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class SparkSumByFold {

	public static void main(String[] args) {
		String master = args.length > 0 ? args[0] : "local[4]";
		
		Integer[] arr = new Integer[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
		
		JavaSparkContext sc = new JavaSparkContext(master, SparkSumByFold.class.getSimpleName());
		
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(arr));
		
		Integer result = rdd.fold(0, new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		// prints 45.
		System.out.println(result);
		
		sc.close();
	}

}
