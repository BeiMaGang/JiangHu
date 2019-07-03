package pageRank;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.Iterator;


public class CorrectFormat {


    public static class FormatMap extends
            Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable PR_INIT = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String leaderName = value.toString().split("\t")[0];
            String followItems = value.toString().split("\t")[1];
            String[] followName = followItems.split("(\\|)|(\\[)");
            // 以|和[作为分隔符

            StringBuilder out = new StringBuilder();
            out.append(new String(leaderName));
            for(int i = 0;i < followName.length;i ++) {
                String s = followName[i].split(",")[0];
                if(s.length() > 0) {
                    out.append("#" + s);   // 使用制表符分隔
                }
            }
            context.write(new Text(out.toString()), PR_INIT);
        }

    }

    public static class FormatReduce extends
            Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {
            Iterator s = values.iterator();
            context.write(key, new Text(s.next().toString()));
        }
    }

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();

        // String[] otherArgs = new String[]{"input/input.txt","output"};
        if (otherArgs.length != 2) {
            System.err.println("Usage:Merge and duplicate removal <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "pageRank.CorrectFormat");
        job.setJarByClass(CorrectFormat.class);
        job.setMapperClass(FormatMap.class);
        job.setReducerClass(FormatReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        for(int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        try {
            FileUtils.deleteDirectory(new File(otherArgs[otherArgs.length - 1]));
        }catch (IOException e) {
            e.printStackTrace();
        }

        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        job.waitForCompletion(true);

    }
}
