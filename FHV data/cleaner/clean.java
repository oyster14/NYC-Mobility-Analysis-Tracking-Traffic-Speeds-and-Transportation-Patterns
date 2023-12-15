package org.cleaner.cleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;

public class clean extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Cleaner <input path> <output path>");
            return 1;
        }

        Job job = Job.getInstance(getConf(), "NYC Taxi Data Cleaner");
        job.setJarByClass(clean.class);
        job.setMapperClass(cleanMapper.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(ExampleInputFormat.class);
        job.setOutputFormatClass(ExampleOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new clean(), args);
        System.exit(res);
    }
}
