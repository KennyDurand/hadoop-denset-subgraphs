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

public class DegreeJob {
    public static float DENSITY = 0;

    public static class NodeDegreeMapper extends Mapper<FloatWritable, Text, IntWritable, IntWritable> {
        public void map(FloatWritable key, Text value, Context context) throws IOException, InterruptedException {
            DegreeJob.DENSITY = key.get();
            String[] edge = value.toString().split(";");

            IntWritable node0 = new IntWritable(Integer.parseInt(edge[0]));
            IntWritable node1 = new IntWritable(Integer.parseInt(edge[1]));
            context.write(node0, node1);
            context.write(node1, node0);
        }
    }

    public static class NodeDegreeReducer extends Reducer<IntWritable, IntWritable, IntWritable, FloatWritable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int degree = 0;

            for (IntWritable val : values) {
            	degree++;
            }

            FloatWritable ratio = new FloatWritable(0);
            if (DegreeJob.DENSITY > 0) {
                ratio.set((float) (degree / (DegreeJob.DENSITY * 2.02)));
            }
            context.write(key, ratio);
      }
    }

    public static Job createJob() throws IOException {
    	Job job = Job.getInstance();

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(NodeDegreeMapper.class);
        job.setReducerClass(NodeDegreeReducer.class);

        job.setInputFormatClass(DensityInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }
}