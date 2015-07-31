import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String year = line.substring(15, 19);
        int horizontalVisibility = Integer.parseInt(line.substring(79,84));
        int verticalVisibility = Integer.parseInt(line.substring(71,75));
        
        int airTemperature;
        if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
            airTemperature = Integer.parseInt(line.substring(88, 92));
        } 
        else {
            airTemperature = Integer.parseInt(line.substring(87, 92));
        }
        String quality = line.substring(92, 93);
        String lat = line.substring(28,34);
        String lon = line.substring(34,41);
        //String rest = line.substring(106); 
        //context.write(new Text(year+"Vvis"+" "+lat+" "+lon), new IntWritable(verticalVisibility));
        //context.write(new Text(year+"Hvis"), new IntWritable(horizontalVisibility));
        if (airTemperature != MISSING && quality.matches("[01459]")) {
            context.write(new Text(year+"temp" + " "+ lat + lon), new IntWritable(airTemperature));
        }
    }
}
