import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    

    String line = value.toString();
    String[] words = line.split(","); 
    if(words.length > 4){
      String city = words[3] ;

      context.write(new Text(city), new IntWritable(1));
    }
    
    
  }
}