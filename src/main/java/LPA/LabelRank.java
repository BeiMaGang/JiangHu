package LPA;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
 * @class name:LabelRank
 * @author:Wu Gang
 * @create: 2019-07-04 15:12
 * @description:
 */
public class LabelRank {
    public static class LabelRankMapper extends Mapper<Text, Text, Text, Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String name = key.toString().split(",")[0];
            String label = key.toString().split(",")[1];
            context.write(new Text(label), new Text(name));
        }
    }

    public static class LabelRankReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text text: values){
                context.write(key, text);
            }
        }
    }

    public static void main(String[] argv) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LPA Rank");
        job.setJarByClass(LabelRank.class);
        job.setMapperClass(LabelRankMapper.class);
        job.setReducerClass(LabelRankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(argv[0]));
        FileOutputFormat.setOutputPath(job, new Path(argv[1]));
        job.waitForCompletion(true);
    }
}
