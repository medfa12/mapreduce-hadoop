package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class StockMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\\s+");  // Assuming space-delimited file

        if (fields.length > 10) {
            String seance = fields[0];
            String code = fields[2];
            String closePrice = fields[5];
            context.write(new Text(code), new Text(closePrice));
        }
    }
}

