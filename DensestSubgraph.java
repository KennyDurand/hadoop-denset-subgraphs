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
    Job densityJob = DensityJob.createJob();

    FileInputFormat.setInputPaths(densityJob, new Path(args[0]));
    FileOutputFormat.setOutputPath(densityJob, new Path(args[1] + "/density"));

    densityJob.waitForCompletion(true);

    Job degreeJob = DegreeJob.createJob();

    FileInputFormat.setInputPaths(degreeJob, new Path(args[1] + "/density"));
    FileOutputFormat.setOutputPath(degreeJob, new Path(args[1] + "/degree"));

    degreeJob.waitForCompletion(true);
  }
}