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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CalculateNodeDegreeJob {
    public static float DENSITY;

    public static class CalculateNodeDegreeMapper extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            CalculateNodeDegreeJob.DENSITY = Float.parseFloat(key.toString());
            String[] edge = value.toString().split(";");

            Text node0 = new Text(edge[0]);
            Text node1 = new Text(edge[1]);
            context.write(node0, node1);
            context.write(node1, node0);
        }
    }

    public static class CalculateNodeDegreeReducer extends Reducer<Text, Text, Text, FloatWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int degree = 0;

            for (Text val : values) {
            	degree++;
            }

            FloatWritable ratio = new FloatWritable(0);
            if (CalculateNodeDegreeJob.DENSITY > 0) {
                ratio.set((float) (degree / CalculateNodeDegreeJob.DENSITY));
            }
            context.write(key, ratio);
      }
    }

    public static Job createJob() throws IOException {
    	Job job = Job.getInstance();

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(CalculateNodeDegreeMapper.class);
        job.setReducerClass(CalculateNodeDegreeReducer.class);

        job.setInputFormatClass(CustomInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }
}