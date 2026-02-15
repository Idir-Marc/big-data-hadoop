package idir.marc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class HadoopJoin {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Hauteur moyenne par espèce");
        job.setJarByClass(ArbresParEspece.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class JoinMapper extends Mapper<LongWritable, Text, Text, TextPairWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TextPairWritable>.Context context) throws IOException, InterruptedException {
            String[] s = value.toString().split("(\b;)+");
            context.write(new Text(s[1]), new TextPairWritable(new Text(s[0]), new Text(s[2])));
        }
    }

    public static class Reduce extends Reducer<Text, TextPairWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<TextPairWritable> values, Reducer<Text, TextPairWritable, Text, Text>.Context context) throws IOException, InterruptedException {
            Map<String, List<String>> map = new HashMap<>();

            for (TextPairWritable pair : values) {
                String tag = pair.getT1().toString();
                String value = pair.getT2().toString();
                map.computeIfAbsent(tag, s -> new ArrayList<>()).add(value);
            }

            List<String> keys = new ArrayList<>(map.keySet());

            // théoriquement un produit cartésien lors d'une jonction pure
            for (int i = 0; i < keys.size(); i++) {
                String k1 = keys.get(i);
                for (int j = i+1; j < keys.size(); j++) {
                    String k2 = keys.get(j);
                    for (String s1 : map.get(k1)) {
                        for (String s2 : map.get(k2)) {
                            context.write(key, new Text(String.format("%s: %s\t%s: %s", k1, s1, k2, s2)));
                        }
                    }
                }
            }
        }
    }

    /// Permet de faire passer deux textes sans risquer que leurs contenus respectifs ne se mélangent entre deux opérations.
    /// Cette classe n'est pas adapté à un résultat final d'opération MapReduce, comme elle écrit les deux textes sans séparation.
    public static class TextPairWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {
        private Text t1;
        private Text t2;

        public TextPairWritable(Text t1, Text t2) {
            this.t1 = Objects.requireNonNull(t1);
            this.t2 = Objects.requireNonNull(t2);
        }

        public TextPairWritable() {
            this.t1 = new Text();
            this.t2 = new Text();
        }

        public Text getT1() {
            return t1;
        }

        public Text getT2() {
            return t2;
        }

        public void setT1(Text t1) {
            this.t1 = Objects.requireNonNull(t1);
        }

        public void setT2(Text t2) {
            this.t2 = Objects.requireNonNull(t2);
        }

        @Override
        public int getLength() {
            return t1.getLength() + t2.getLength();
        }

        @Override
        public byte[] getBytes() {
            byte[] b2 = t2.getBytes();
            byte[] b1 = t1.getBytes();
            byte[] b = Arrays.copyOf(b1, b1.length + b2.length);
            System.arraycopy(b2, 0, b, b1.length, b2.length);
            return b;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            t1.write(dataOutput);
            t2.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            t1.readFields(dataInput);
            t2.readFields(dataInput);
        }
    }
}
