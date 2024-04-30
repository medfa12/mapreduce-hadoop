package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;

public class Main extends Configured implements Tool {
    static class Pair {
        public int start;
        public int end;

        public Pair(int start, int end) {
            this.start = start;
            this.end = end;
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: Main <input file> <output file>");
            return 1; // Error
        }

        String inputFile = args[0];
        String outputFile = args[1];

        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(inputFile);
        Path outputPath = new Path(outputFile);

        try (FSDataInputStream fis = fs.open(inputPath);
             BufferedReader br = new BufferedReader(new InputStreamReader(fis));
             FSDataOutputStream fos = fs.create(outputPath, true)) {

            ArrayList<String> lines = new ArrayList<>();
            String line;
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }

            ArrayList<Pair> bounds = new ArrayList<>();
            String header = lines.get(1);
            int start = 0;
            int pos = start;
            while (pos < header.length()) {
                while (pos < header.length() && header.charAt(pos) == '-') {
                    pos++;
                }
                if (header.charAt(start) != '-') {
                    break;
                }
                bounds.add(new Pair(start, pos));
                start = pos + 1;
                pos++;
            }

            for (int i = 0; i < lines.size(); i++) {
                if (i == 1) continue;
                StringBuilder newLine = new StringBuilder();
                for (Pair bound : bounds) {
                    if (lines.get(i).length() >= bound.end) {
                        newLine.append(lines.get(i).substring(bound.start, bound.end).trim()).append(";");
                    } else {
                        newLine.append(";");
                    }
                }
                fos.write((newLine.toString().replaceAll(";$", "") + "\n").getBytes());
            }
        }
        return 0; // Success
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Main(), args);
        System.exit(exitCode);
    }
}
