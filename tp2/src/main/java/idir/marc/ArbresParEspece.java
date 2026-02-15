package idir.marc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class ArbresParEspece {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Arbres par espèce");
        job.setJarByClass(ArbresParEspece.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            String[] s = value.toString().split("(\b;)+");
            if (s.length < 3) return;
            context.write(new Text(s[2]), NullWritable.get());
        }
    }

    public static class Reduce extends Reducer<Text, Object, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<Object> values, Reducer<Text, Object, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            context.write(key, new LongWritable(StreamSupport.stream(values.spliterator(), false).count()));
        }
    }
}
