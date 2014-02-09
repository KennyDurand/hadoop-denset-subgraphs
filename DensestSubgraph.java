package sogara.hadoop.subgraph;

import java.io.IOException;
import java.lang.InterruptedException;
import java.lang.ClassNotFoundException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class DensestSubgraph {

  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    Job job = DensityJob.createJob();

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }
}