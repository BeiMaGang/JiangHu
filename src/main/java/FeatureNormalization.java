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
import java.util.LinkedList;

/*
 * @class name:FeatureNormalization
 * @author:Wu Gang
 * @create: 2019-07-01 15:51
 * @description:
 */
public class FeatureNormalization {

    public static class NormalizationMapper extends Mapper<Text, Text, Text, Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String tmp = key.toString().replace("<","").replace(">", "");
            String realKey = tmp.split(",")[0];
            String realValue = tmp.split(",")[1];
            realValue = realValue + "," + value.toString();
              context.write(new Text(realKey), new Text(realValue));
        }
    }

    public static class NormalizationReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            LinkedList<String> list = new LinkedList<>();
            for(Text text: values){
                list.add(text.toString());
                sum += Double.parseDouble(text.toString().split(",")[1]);
            }
            StringBuilder value = new StringBuilder("[");
            for(String text: list){
                value.append(text.split(",")[0]).append(",");
                double i = Double.parseDouble(text.split(",")[1]);
                value.append(i / sum);
                value.append("|");
            }
            value.replace(value.length() - 1, value.length(),"]");
            context.write(key, new Text(value.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Graph Construction and Feature Normalization");
        job.setJarByClass(FeatureNormalization.class);
        job.setMapperClass(NormalizationMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(NormalizationReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
