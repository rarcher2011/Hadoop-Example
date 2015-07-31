import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,    InterruptedException {
        int maxValue = Integer.MIN_VALUE;
        int minValue = Integer.MAX_VALUE;
        int sum1=0;
        int sum2 = 0;
        int num = 0;
        for (IntWritable value : values) {
        	maxValue = Math.max(maxValue, value.get());
       		minValue = Math.min(minValue, value.get());	
            sum1 = (value.get()<9999) ? sum1+value.get() : sum1+0;
            sum2 = sum2 + value.get();
            num++;
        }
        int checkKey = key.find("temp");

        //context.write(key, new IntWritable(sum1));
        //context.write(key, new IntWritable(sum2));
        context.write(key, new IntWritable(num));
        if(checkKey >=0){
            context.write(key, new IntWritable(maxValue));
            context.write(key, new IntWritable(minValue));
            //context.write(key, new IntWritable((sum1/num)));
        	context.write(key, new IntWritable((sum2/num)));
        }
        
    }
}   
