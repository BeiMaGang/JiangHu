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
 * @class name:InitLabel
 * @author:Wu Gang
 * @create: 2019-07-04 13:13
 * @description:
 */
public class InitLabel {
    public static class InitLabelMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String tmp = key.toString().replace("<","").replace(">", "");
            String realKey = tmp.split(",")[0];
            String realValue = tmp.split(",")[1];
            realValue = realValue + "," + value.toString();
            context.write(new Text(realKey), new Text(realValue));
        }
    }

    public static class InitLabelReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder value = new StringBuilder("");
            for(Text text: values){
                value.append(text.toString());
                value.append("|");
            }
            value.replace(value.length() - 1, value.length(),"");
            context.write(new Text(key.toString() + "," + key.toString()), new Text(value.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Graph Construction and Feature Normalization");
        job.setJarByClass(InitLabel.class);
        job.setMapperClass(InitLabelMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(InitLabelReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
