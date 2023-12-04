import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import  org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.parquet.Log;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;

public class NYCTaxiDataClean extends Configured implements Tool {
    private static final Log LOG = new Log(NYCTaxiDataClean.class);

    public int run(String[] args) throws Exception {
        if(args.length < 2) {
            LOG.error("Usage: " + getClass().getName() + " INPUTFILE OUTPUTFILE [compression]");
            return 1;
        }
        String inputFile = args[0];
        String outputFile = args[1];
        String compression = (args.length > 2) ? args[2] : "none";
        
        String writeSchema = "message new_schema { " +
                "required int64 vendor_id; " +
                "required int64 pickup_datetime (TIMESTAMP(MICROS,false)); " +
                "required int64 dropoff_datetime (TIMESTAMP(MICROS,false)); " +
                "required int64 passenger_count; " +
                "required double trip_distance; " +
                "required int64 pickup_locationId; " +
                "required int64 dropoff_locationId; " +
                "required int64 payment_type; " +
                "required double fare_amount; " +
                "required double extra; " +
                "required double mta_tax; " +
                "required double tip_amount; " +
                "required double tolls_amount; " +
                "required double improvement_surcharge; " +
                "required double total_amount; " +
                "required double congestion_surcharge; " +
                "required double airport_fee; " +
                "}";
        MessageType schema = MessageTypeParser.parseMessageType(writeSchema);
        LOG.info(schema);
        GroupWriteSupport.setSchema(schema, getConf());

        Job job = new Job(getConf());
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());
        job.setMapperClass(NYCTaxiDataCleanMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(ExampleInputFormat.class);
        job.setOutputFormatClass(ExampleOutputFormat.class);

        CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
        if(compression.equalsIgnoreCase("snappy")) {
            codec = CompressionCodecName.SNAPPY;
        } else if(compression.equalsIgnoreCase("gzip")) {
            codec = CompressionCodecName.GZIP;
        }
        LOG.info("Output compression: " + codec);
        ExampleOutputFormat.setCompression(job, codec);

        FileInputFormat.setInputPaths(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        try {
            int res = ToolRunner.run(new Configuration(), new NYCTaxiDataClean(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }


}