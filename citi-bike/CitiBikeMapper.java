import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;

public class MaxTemperatureMapper
    extends Mapper<LongWritable, Text, Text, Text> {
  
  SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS");
  SimpleDateFormat outputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH");
  Calendar calendar = Calendar.getInstance();

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString().replaceAll("\"", "");
    String[] parts = line.split(",")

    if (parts.length == 14) {
        try {
            int bikeId = Integer.parseInt(parts[0]);
            Date startTime = dateFormat.parse(parts[1]);
            Date endTime = dateFormat.parse(parts[2]);
            int startStationId = Integer.parseInt(parts[3]);
            String startStationName = parts[4];
            double startStationLat = Double.parseDouble(parts[5]);
            double startStationLon = Double.parseDouble(parts[6]);
            int endStationId = Integer.parseInt(parts[7]);
            String endStationName = parts[8];
            double endStationLat = Double.parseDouble(parts[9]);
            double endStationLon = Double.parseDouble(parts[10]);
            String userType = parts[11];
            int birthYear = Integer.parseInt(parts[12]);
            int tripDuration = Integer.parseInt(parts[13]);

            calendar.setTime(startTime);
            String extractedDate = outputDateFormat.format(startTime);
            long timeDifferenceMillis = startTime.getTime() - endTime.getTime()
            long timeDifferenceSeconds = timeDifferenceMillis / 1000;
            int hour = calendar.get(Calendar.HOUR_OF_DAY);
            
            String outputValue = String.format("%d,%f,%f,%f,%f", timeDifferenceSeconds, startStationLat, startStationLon, endStationLat, endStationLon);
            
            // Emit the bikeId as the key and the parsed values as the value
            context.write(new Text(hour.toString())), new Text(outputValue));
        } catch (NumberFormatException e) {
            // Handle parsing errors
            e.printStackTrace();
        }
  }
}