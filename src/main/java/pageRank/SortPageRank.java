package pageRank;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.IOException;


public class SortPageRank {


    public static class SortMap extends
            Mapper<Object, Text, DoubleWritable, Text> {

        private static final IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            double val = Double.valueOf(value.toString().split("\t")[1]);   // 获取pr值
            String leaderName = (value.toString().split("\t")[0]).split("#")[0]; // 获取首个名字
            context.write(new DoubleWritable(val), new Text(leaderName));
        }

    }

    public static class SortComparator extends DoubleWritable.Comparator {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class SortReduce extends
            Reducer<DoubleWritable, Text, Text, Text> {

        public void reduce(DoubleWritable key, Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {
            for (Text it : values) {
                context.write(new Text(it.toString()), new Text(key.toString()));
            }
        }
    }

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();

        // String[] otherArgs = new String[]{"input/input_3.txt","output"};
        if (otherArgs.length != 2) {
            System.err.println("Usage:Merge and duplicate removal <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "pageRank.SortPageRank");
        job.setJarByClass(SortPageRank.class);
        job.setMapperClass(SortMap.class);
        job.setReducerClass(SortReduce.class);
        job.setSortComparatorClass(SortComparator.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        try {
            FileUtils.deleteDirectory(new File(otherArgs[otherArgs.length - 1]));
        } catch (IOException e) {
            e.printStackTrace();
        }

        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        job.waitForCompletion(true);
    }
}
