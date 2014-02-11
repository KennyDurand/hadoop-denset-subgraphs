package sogara.hadoop.subgraph;

import java.util.*;
import java.io.IOException;
import java.lang.InterruptedException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DeleteExtremityNodeJob {
    private final static double RATIO = 2.02;
    
    public static class DeleteExtremityNodeMapper extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            if (split.getPath().toString().contains("/degree/")) {
                if (Float.parseFloat(value.toString()) < 2.02) {
                    context.write(key, new Text("$"));
                }
            } else {
                String[] edge = value.toString().split(";");
                context.write(value, key);
            }
        }
    }

    public static class DeleteExtremityNodeReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<Text> nodeList = new ArrayList<Text>();

            for (Text val : values) {
                if (val.toString().equals("$")) {
                    nodeList = new ArrayList<Text>();
                    break;
                } else {
                    nodeList.add(new Text(val));
                }
            }

            for (Text node : nodeList) {
                context.write(key, node);
            }
      }
    }

    public static Job createJob() throws IOException {
        Job job = Job.getInstance();

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(DeleteExtremityNodeMapper.class);
        job.setReducerClass(DeleteExtremityNodeReducer.class);

        job.setInputFormatClass(CustomInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }
}