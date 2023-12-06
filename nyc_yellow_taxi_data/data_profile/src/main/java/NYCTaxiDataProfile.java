// mvn clean package
// hadoop jar target/NYCTaxiTripDataProfile-1.0-SNAPSHOT.jar NYCTaxiDataProfile rbda_project/cleaned_nyc_taxi_dataset rbda_project/2021_profile_result


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import  org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.parquet.Log;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.example.data.Group;

public class NYCTaxiDataProfile extends Configured implements Tool {
    private static final Log LOG = new Log(NYCTaxiDataProfile.class);

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            LOG.error("Usage: " + getClass().getName() + " INPUTFILE OUTPUTFILE");
            return 1;
        }
        String inputFile = args[0];
        String outputFile = args[1];

        // Set up the Hadoop Map Reduce task
        Job job = Job.getInstance(getConf());
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        job.setMapperClass(NYCTaxiDataProfileMapper.class);
        job.setReducerClass(NYCTaxiDataProfileReducer.class);

        job.setInputFormatClass(ExampleInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        try {
            int res = ToolRunner.run(new Configuration(), new NYCTaxiDataProfile(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }
}