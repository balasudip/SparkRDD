import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;



import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkRDD 
{

	public static void main(String args[])throws IOException
	{
		if (args.length == 0)
		{
			System.out.println("No files provided.");
			System.exit(0);
		}
		SparkConf c = new SparkConf().setAppName("xyz").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(c);
		JavaRDD<String> inFile = sc.textFile(agrs[0]);
		JavaPairRDD<Integer, String> numLines = inFile.mapToPair(new PairFunction<String, Integer, String>()
		{
			@Override
			public Tuple2<Integer, String> call(String s)
			{
				String code = s.split("\t")[5];
				return new Tuple2<Integer, String>(Integer.valueOf(code), s);
			}
		});
		
		Map<Integer, Long> x1 = numLines.countByKey();
		System.out.println(x1);
		JavaPairRDD<Integer, Long> addB = inFile.mapToPair(new PairFunction<String, Integer, Long>()
		{
			@Override
			public Tuple2<Integer, Long> call(String s)
			{
				String code = s.split("\t")[5];
				Long size = Long.valueOf(s.split("\t")[6]);
				return new Tuple2<Integer, Long>(Integer.valueOf(code), size);
			}
		});
		JavaPairRDD<Integer, Long> sbc = addB.reduceByKey(new Function2<Long, Long, Long>() 
		{
			@Override
			public Long call(Long s1, Long s2) throws Exception
			{
			return s1 + s2;
			} 
		});
			
		BufferedWriter w1 = null;
		w1 = new BufferedWriter( new FileWriter( "task1.txt"));	
		Map<Integer, Long> x1 = addB.countByKey();
		System.out.println(sbc.collectAsMap());
		Map<Integer, Long> sbcMap = sbc.collectAsMap();
		for (Integer code : x1.keySet()) 
		{
			long sum = sbcMap.get(code);
			long count = x1.get(code);
			double average = (double)sum / count;
			System.out.printf("Code %d average is %f\n", code, average);
			String toWrite = "Code " + Integer.toString(code) + ", average number of bytes = " + Double.toString(average)+ "\n";
			w1.write(toWrite);
		}
		w1.close();
		JavaPairRDD<Tuple2<String, String>, String> mLines = inFile.mapToPair((PairFunction<String, Tuple2<String, String>, String>) s -> {

		String[] p1 = s.split("\t");
		return new Tuple2<>(new Tuple2<>(p1[0], p1[4]), s);
		});
			
		JavaPairRDD<Tuple2<String, String>, Iterable<String>> gl = mLines.groupByKey();
		JavaRDD<Tuple2<String, String>> jr = gl.flatMap(new FlatMapFunction<Tuple2<Tuple2<String, String>, Iterable<String>>, Tuple2<String, String>>() 
		{
			@Override 
			public Iterator<Tuple2<String, String>> call(Tuple2<Tuple2<String, String>, Iterable<String>> input) throws Exception 
			{
				ArrayList<String> l1 = new ArrayList<String>();
				for (String line : input._2())
					l1.add(line);
				ArrayList<Tuple2<String, String>> answer = new ArrayList<>();
				for (int i = 0; i < l1.size(); i++) 
				{
					int mn1 = Integer.parseInt(l1.get(i).split("\t")[2]);
					for (int j = i + 1; j < l1.size(); j++) 
					{
						int mn2 = Integer.parseInt(l1.get(j).split("\t")[2]);
						if (Math.abs(mn1 - mn2) < 60 * 60 * 1000) 
						{
							answer.add(new Tuple2(l1.get(i), l1.get(j)));
						}
					}
				}
				return answer.iterator();
			}
		});
		jr.saveAsTextFile("task2.txt");

		}
	}
		
}