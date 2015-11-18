package MP.WordCount;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public WordCount() {
		// TODO Auto-generated constructor stub
	}

	
	public static void main(String[] args) throws Exception {
		/* 初始化 */
		
		String uri = "hdfs://c3master:8020" ;
		Configuration conf = new Configuration();           
		FileSystem fs = FileSystem. get(URI.create (uri), conf);

		/* 建立MapReduce Job, 該job的名稱為MyWordcount */
		Job job = new Job(conf, "SowayCount"); 

		/* 啟動job的jar class 為MyWordcount */
		job.setJarByClass(WordCount.class);
		/* 啟動job的map class 為MyMapper */
		job.setMapperClass(MyMapper.class);
		/* 啟動job的reduce class 為MyReducer */
		job.setReducerClass(MyReducer.class);

		/* 輸入資料的HDFS路徑 */
		FileInputFormat.addInputPath(job, new Path("hdfs://c3master:8020/input02"));
		/* 輸出資料的HDFS路徑 */
		FileOutputFormat.setOutputPath(job, new Path("/output03"));

		/* 輸出Key的型別 */
		job.setOutputKeyClass(Text.class);
		/* 輸出Value的型別 */
		job.setOutputValueClass(IntWritable.class);

		/* 啟動Job並回傳是否成功執行完畢 */
		System.exit(job.waitForCompletion(true)? 0 : 1);
		System.out.println("OK");
	}

	static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			/* 將每一行的字串存到lineValue */
			System.out.println("map content:" + key.get() + "and" + value.toString());
			String lineValue = value.toString();

			/* 用StringTokenizer分割有空白、跳行等字元 */
			StringTokenizer stk = new StringTokenizer(lineValue);

			/*
			 * 將每個切割的字串存到wordValue, 再將wordValue設為Reduce的Key, value設為整數1 (one)
			 */
			while (stk.hasMoreTokens()) {
				String wordValue = stk.nextToken();
				word.set(wordValue);
				context.write(word, one);
			}
		}
	}

	static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			/* 累加該單字的數量 */
			int sum = 0;

			/* 在Iterable變數values用迴圈方式,將每個值(整數1)取出並累加 */
			for (IntWritable value : values) {
				sum += value.get();
			}

			/* 將累加的結果存到result */
			result.set(sum);

			/* 輸出計算的結果 */
			context.write(key, result);
		}
	}
}
