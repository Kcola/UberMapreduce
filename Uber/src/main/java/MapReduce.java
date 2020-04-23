import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class MapReduce {
    public static class AverageMapper
            extends Mapper<Object, Text, Text, DoubleWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String line = value.toString();
            String[] lineArray = line.split(",");
            int hour = Integer.parseInt(lineArray[2]);
            double averageSpeed = Double.parseDouble(lineArray[9]);
            context.write(new Text(fileName + ": " +hour), new DoubleWritable(averageSpeed));
        }
    }
    public static class AverageReducer
            extends Reducer<Text, DoubleWritable,Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for(DoubleWritable speed :values){
                sum = sum+speed.get();
                count++;
            }
                context.write(key, new DoubleWritable(sum/count));
        }
    }
    public static class BestTimeMapper
            extends Mapper<Object, Text, Text, Text>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineArray = line.split(": ");
            String location =lineArray[0].split("\\.")[0];
            String hourToSpeed = lineArray[1].replace("\t", "_");
            context.write(new Text(location), new Text(hourToSpeed));
        }
    }
    public static class BestTimeReducer
            extends Reducer<Text, Text,Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            double max = 0;
            double min = Double.MAX_VALUE;
            String max_hour = "";
            String min_hour = "";
            for(Text hrToSpeed :values){
                if(max < Double.parseDouble(hrToSpeed.toString().split("_")[1])){
                    max =   Double.parseDouble(hrToSpeed.toString().split("_")[1]);
                    max_hour =  hrToSpeed.toString().split("_")[0];
                }
                if(min > Double.parseDouble(hrToSpeed.toString().split("_")[1])){
                    min =   Double.parseDouble(hrToSpeed.toString().split("_")[1]);
                    min_hour =  hrToSpeed.toString().split("_")[0];
                }
            }
            context.write(key, new Text(max_hour+", "+max+", "+min_hour+", "+min));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "average speed per town");
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(AverageMapper.class);
        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        Path ofile = new Path(otherArgs[0]);
        FileSystem fs = ofile.getFileSystem(conf);
        FileStatus[] fileStatus = fs.listStatus(new Path(otherArgs[0]));
        for (FileStatus status : fileStatus) {
            TextInputFormat.addInputPath(job, new Path(status.getPath().toString()));
        }
        TextOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]+"/averages"));
        System.out.println("Starting job 1");
        job.waitForCompletion(true);
        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "best and worst time to drive per town");
        job2.setJarByClass(MapReduce.class);
        job2.setMapperClass(BestTimeMapper.class);
        job2.setReducerClass(BestTimeReducer.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setMapOutputKeyClass(Text.class);
        TextInputFormat.addInputPath(job2, new Path(otherArgs[otherArgs.length - 1]+"/averages"));
        TextOutputFormat.setOutputPath(job2,
                new Path(otherArgs[otherArgs.length - 1]+"/best_time"));
        System.out.println("Starting job 2");
        job2.waitForCompletion(true);
    }
}
