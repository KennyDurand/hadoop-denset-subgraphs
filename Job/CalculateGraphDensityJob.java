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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CalculateGraphDensityJob {
    public static class CalculateGraphDensityMapper extends Mapper<Text, Text, IntWritable, Text> {
        private final static IntWritable one = new IntWritable(1);

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Text edge = new Text(key.toString() + ";" + value.toString());

            context.write(one, edge);
        }
    }

    public static class CalculateGraphDensityReducer extends Reducer<IntWritable, Text, FloatWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text edge;
            ArrayList<Text> edges = new ArrayList<Text>();
            ArrayList<Integer> nodes = new ArrayList<Integer>();

            for (Text val : values) {
            	edge = new Text(val.toString());
                edges.add(edge);
                String[] nodeArray = edge.toString().split(";");
                int node0 = Integer.parseInt(nodeArray[0]);
                int node1 = Integer.parseInt(nodeArray[1]);
                if (!nodes.contains(node0)) {
                    nodes.add(node1);
                }
                if (!nodes.contains(node0)) {
                    nodes.add(node1);
                }
            }

            FloatWritable density = new FloatWritable(0);
            if (edges.size() > 0) {
                density.set((((float) edges.size()) / Collections.max(nodes)));
            }
            for (Text edg : edges) {
                context.write(density, edg);    
            }
      }
    }

    public static Job createJob() throws IOException {
        Job job = Job.getInstance();

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(CalculateGraphDensityMapper.class);
        job.setReducerClass(CalculateGraphDensityReducer.class);

        job.setInputFormatClass(CustomInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }
}