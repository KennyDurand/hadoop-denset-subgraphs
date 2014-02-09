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

public class DensityJob {
    public static class GraphDensityMapper extends Mapper<IntWritable, IntWritable, IntWritable, Text> {
        private final static IntWritable one = new IntWritable(1);

        public void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            Text edge = new Text(key.toString() + ";" + value.toString());

            context.write(one, edge);
        }
    }

    public static class GraphDensityReducer extends Reducer<IntWritable, Text, FloatWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text edge;
            ArrayList<Text> edges = new ArrayList<Text>();
            ArrayList<Integer> nodes = new ArrayList<Integer>();

            for (Text val : values) {
            	edge = new Text(val.toString());
                edges.add(edge);
                String[] nodeArray = edge.toString().split(";");
                nodes.add(Integer.parseInt(nodeArray[0]));
                nodes.add(Integer.parseInt(nodeArray[1]));
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

        job.setMapperClass(GraphDensityMapper.class);
        job.setReducerClass(GraphDensityReducer.class);

        job.setInputFormatClass(GraphInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }
}