package org.rbda;
import java.io.IOException;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.stream.Collectors;
import java.io.IOException;
import org.apache.parquet.Log;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
public class reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Map<Integer, Integer> departureFrequency = new HashMap<>();
    private IntWritable one = new IntWritable(1);

    private int maxPickupFrequency = 0;
    private String maxPickupLocation = "";
    private int maxDropoffFrequency = 0;
    private String maxDropoffLocation = "";

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for (IntWritable value : values) {
            sum+=1;

//            try{
//                int pick = convertStringToInteger(value.toString());
//                departureFrequency.put(pick, departureFrequency.getOrDefault(pick, 0) + 1);
//
//            }catch (NumberFormatException e){
//                ;
//            }

        }
        context.write(key,new IntWritable(sum));
        String keyStr = key.toString();
        if (keyStr.startsWith("pickup")) {
            if (sum > maxPickupFrequency) {
                maxPickupFrequency = sum;
                maxPickupLocation = keyStr;
            }
        } else if (keyStr.startsWith("dropoff")) {
            if (sum > maxDropoffFrequency) {
                maxDropoffFrequency = sum;
                maxDropoffLocation = keyStr;
            }
        }
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("Max Pickup Location: " + maxPickupLocation), new IntWritable(maxPickupFrequency));
        context.write(new Text("Max Dropoff Location: " + maxDropoffLocation), new IntWritable(maxDropoffFrequency));

    }


    public static int convertStringToInteger(String str) throws NumberFormatException {
        try {
            double doubleValue = Double.parseDouble(str);
            return (int) doubleValue;
        } catch (NumberFormatException e) {
            throw new NumberFormatException("String '" + str + "' cannot be converted to an integer: " + e.getMessage());
        }
    }
}