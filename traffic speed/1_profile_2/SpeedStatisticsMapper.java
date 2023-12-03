import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SpeedStatisticsMapper
        extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith("ID")) {
            return;
        }
        String[] parts = line.split(",");
        String year = parts[4].substring(6, 10);
        if (!year.equals("2021")) {
            return;
        }
        context.write(NullWritable.get(), new Text(parts[1]));
    }
}
