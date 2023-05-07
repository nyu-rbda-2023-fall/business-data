import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Average {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: Average <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = Job.getInstance();
    job.setJarByClass(Average.class);
    job.setJobName("Average");
    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(AverageMapper.class);
    //job.setCombinerClass(AverageReducer.class);
    job.setReducerClass(AverageReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}