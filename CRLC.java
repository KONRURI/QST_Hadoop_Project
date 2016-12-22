package RoundTwo;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//flower
public class CRLC {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String pattern = "(\\d+.\\d+.\\d+.\\d+) [^ ]* [^ ]* \\[([^ ]* [^ ]*)\\] \"[^ ]+ ([^ ]+) .*\" \\d+ \\d+ \"(.*)\" \"(.*)\"";
			Pattern p=Pattern.compile(pattern);
			String line=value.toString();
			Matcher m=p.matcher(line);
			if(m.find()){
				String ip=m.group(1);
				String time=m.group(2);
				String everyday=time.split(":")[0];
				context.write(new Text(everyday), new Text(ip));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
		Set<String> now=new HashSet<>();
		Set<String> last=new HashSet<>();
		Set<String> temp=new HashSet<>();
		@Override
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(Text val:value){
				now.add(val.toString());
				}
			last.addAll(temp);//存昨天的数据
			temp.clear();//清除上一次的数据
			temp.addAll(now);//temp里面存储今天的全部数据
			now.retainAll(last);//求交集
			
			if (last.size()==0) {
				context.write(key , new DoubleWritable());
			}else{
				double CRLC = ((double)now.size() / last.size());
				context.write(key , new DoubleWritable(CRLC));
			}
			
			now.clear();
			last.clear();
		}

	}

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CRLC");
		job.setJarByClass(CRLC.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
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
