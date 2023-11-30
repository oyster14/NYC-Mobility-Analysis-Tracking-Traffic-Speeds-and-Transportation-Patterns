import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

public class Filter1Mapper
        extends Mapper<LongWritable, Text, NullWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith("ID")) {
            return;
        }
	CSVParser parser = new CSVParserBuilder()
                .withSeparator(',')
                .withQuoteChar('\"')
                .build();
	String[] columns = parser.parseLine(line);
        String year = columns[4].substring(6, 10);
        if (!year.equals("2021")) {
            return;
        }
        String[] latlons = columns[6].split(" ");
        double latSum = 0;
        double lonSum = 0;
	int invalid = 0;
        for (String latlon : latlons) {
            String[] ll = latlon.split(",");
	    if (ll.length != 2) {
		invalid++;
                continue;
	    }
	    double lat,lon;
	    try {
            	lat = Double.parseDouble(ll[0]);
            	lon = Double.parseDouble(ll[1]);
	    } catch (NumberFormatException e) {
                invalid++;
		continue;
            }
	    latSum += lat;
	    lonSum += lon;
        }
	int valid = latlons.length - invalid;
	if (valid == 0) {
            return ;
	}
        latSum /= valid;
        lonSum /= valid;

        StringBuilder sb = new StringBuilder();
        sb.append(columns[1]);
        sb.append(",");
        sb.append(columns[4]);
        sb.append(",");
        sb.append(Double.toString(latSum));
        sb.append(",");
        sb.append(Double.toString(lonSum));

        context.write(NullWritable.get(), new Text(sb.toString()));
    }
}

