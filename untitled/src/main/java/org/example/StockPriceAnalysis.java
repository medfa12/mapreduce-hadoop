package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;

public class StockPriceAnalysis {

    public static class StockPriceMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(";");
            if (parts.length > 6) {
                try {
                    String stock = parts[3].trim();
                    double closePrice = Double.parseDouble(parts[6].trim());
                    context.write(new Text(stock), new DoubleWritable(closePrice));
                } catch (NumberFormatException e) {
                    // Handle parse error or ignore
                }
            }
        }
    }

    public static class StockPriceReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<Double> priceList = new ArrayList<>();
            for (DoubleWritable val : values) {
                priceList.add(val.get());
            }
            double sum = 0;
            for (double price : priceList) {
                sum += price;
            }
            double mean = sum / priceList.size();
            double sumOfSquares = 0;
            for (double price : priceList) {
                sumOfSquares += (price - mean) * (price - mean);
            }
            double stddev = Math.sqrt(sumOfSquares / priceList.size());
            context.write(key, new Text("Mean: " + mean + ", StdDev: " + stddev));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: stockanalysis <in1> [<in2> ... <inN>] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Stock Price Analysis");
        job.setJarByClass(StockPriceAnalysis.class);
        job.setMapperClass(StockPriceMapper.class);
        job.setReducerClass(StockPriceReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
