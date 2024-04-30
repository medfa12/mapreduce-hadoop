package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class StockReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;
        int count = 0;
        for (Text value : values) {
            sum += Double.parseDouble(value.toString());
            count++;
        }
        double average = sum / count;
        context.write(key, new Text(String.format("%.2f", average)));
    }
}
