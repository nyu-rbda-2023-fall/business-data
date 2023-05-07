import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageMapper
    extends Mapper<LongWritable, Text, Text, FloatWritable> {

  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    

    String line = value.toString();
    String[] words = line.split(","); 
    float stars = 3;
    if(words.length > 4){
      String city = words[3];
      //float stars = Float.parseFloat(words[8]);
      try {
         stars = Float.parseFloat(words[8]);
    	} catch (NumberFormatException e){
      }

      context.write(new Text(city), new FloatWritable(stars));
    }
    
    
  }
}