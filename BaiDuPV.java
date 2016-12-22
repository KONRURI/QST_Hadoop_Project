package RoundTwo;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class BaiDuPV {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line=value.toString();
			String pattern="(\\d+.\\d+.\\d+.\\d+) ([^ ]*) ([^ ]*) \\[([^ ]* [^ ]*)\\] \"([^ ]+ [^ ]+ [^ ]+)\" (\\d+) (\\d+) \"([^\"]*)\" \"[^ ]* [^ ]* [^ ]* \\+[^ ]*\\:\\//([^ ]*\\/[^ ]*)\\)\"";
			Pattern p=Pattern.compile(pattern);
			Matcher m=p.matcher(line);
			if(m.find()){
				context.write(new Text("1"), new Text(m.group(4)));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum=0;
			while(value.iterator().hasNext()){
				Text text=new Text(value.iterator().next());
				sum++;
			}
			context.write(new Text("百度跳转率:"), new Text(String.valueOf(sum)));
			

		}
	}

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "jumpjump");
		job.setJarByClass(BaiDuPV.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
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
