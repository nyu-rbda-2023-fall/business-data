import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer
    extends Reducer<Text, FloatWritable, Text, FloatWritable> {
  
  @Override
  public void reduce(Text key, Iterable<FloatWritable> values, Context context)
      throws IOException, InterruptedException {
    
    float starsum = 0;
    float loopcount = 0;
    for (FloatWritable value : values) {
      starsum += value.get();
      loopcount+= 1;
    }

    if(loopcount!= 0){
    	float avg = starsum/loopcount;

    	context.write(key, new FloatWritable(avg));
    }
    
  }
} 