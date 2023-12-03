import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MaxMinDateReducer
        extends Reducer<NullWritable, Text, NullWritable, Text> {
    private SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
    private Text maxText = new Text();
    private Text minText = new Text();

    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Date maxDate = null;
        Date minDate = null;
        try {
            for (Text value : values) {
                Date currentDate = dateFormat.parse(value.toString());
                if (maxDate == null || currentDate.after(maxDate)) {
                    maxDate = currentDate;
                }
                if (minDate == null || currentDate.before(minDate)) {
                    minDate = currentDate;
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if (maxDate != null) {
            maxText.set(dateFormat.format(maxDate));
            context.write(NullWritable.get(), maxText);
        }
        if (minDate != null) {
            minText.set(dateFormat.format(minDate));
            context.write(NullWritable.get(), minText);
        }
    }
}
