package RoundTwo;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HourUV {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			SimpleDateFormat regularFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Configuration conf = context.getConfiguration();
			String M = conf.get("beginTime");
			String N = conf.get("endTime");
			//System.out.println(M);
			Date beginDate = new Date(0);
			Date endDate = new Date(0);
			try {
				beginDate = regularFormat.parse(M);
				endDate = regularFormat.parse(N);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Locale locale = Locale.US; // 语言信息美国化
			SimpleDateFormat inputFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", locale);
			String pattern = "(\\d+.\\d+.\\d+.\\d+) [^ ]* [^ ]* \\[([^ ]* [^ ]*)\\] \"[^ ]+ ([^ ]+) .*\" \\d+ \\d+ \"(.*)\" \"(.*)\"";
			Pattern p = Pattern.compile(pattern);
			String line = value.toString();
			Matcher m = p.matcher(line);
			if (m.find()) {
				String IP = m.group(1);
				String Time = m.group(2);
				Date dateTime = new Date(0);
				try {
					dateTime = inputFormat.parse(Time);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (dateTime.before(endDate) && dateTime.after(beginDate)) {
					context.write(new Text(IP), new IntWritable(1));
					System.out.println(IP);
				}
			}
		}

	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		int count = 0;

		@Override
		protected void reduce(Text key, Iterable<IntWritable> value, Context context)
				throws IOException, InterruptedException {
			count++;
		}

		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(new Text("UV"), new IntWritable(count));
		}
	}

	public static void main(String[] args) throws IOException {
		//System.setProperty("hadoop.home.dir" , "C:\\实训\\hadoop-common-2.2.0-bin-master\\" );
		Configuration conf = new Configuration();
		conf.set("beginTime", args[2]);
		conf.set("endTime", args[3]);
		Job job = Job.getInstance(conf, "HourUV");
		job.setJarByClass(HourUV.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);// 提交job成功，推出JVM虚拟机。
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}