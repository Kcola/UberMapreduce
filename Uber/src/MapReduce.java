import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class MapReduce {
    public static class TimeMapper
            extends Mapper<Object, Text, IntWritable, DoubleWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
/*
2 = hour of day
9 = speed mph mean
*/
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String line = value.toString();
            String[] lineArray = line.split(",");
            int hour = Integer.parseInt(lineArray[2]);
            double averageSpeed = Double.parseDouble(lineArray[9]);
            context.write(new IntWritable(hour), new DoubleWritable(averageSpeed));
        }
    }
    public static class TimeReducer
            extends Reducer<IntWritable, DoubleWritable,IntWritable, DoubleWritable> {

        public void reduce(IntWritable key, Iterable<DoubleWritable> values,
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
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "average speed");
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(MapReduce.TimeMapper.class);
        job.setReducerClass(MapReduce.TimeReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
