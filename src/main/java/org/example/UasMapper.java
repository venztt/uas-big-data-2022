package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class UasMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>{

    private LongWritable studyWritable = new LongWritable(0);
    private Text studentText = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
            throws IOException
    {
        String line = value.toString();

        System.out.println("Ini adalah isi dari line");
        System.out.println(line);

        // Pecah dulu berdasarkan '\t'
        String[] split = line.split(",");
        String student = split[0];
        String study = split[5];


        // Laporkan ke kolektor
        this.studentText.set(student);
        this.studyWritable.set(Integer.parseInt(study));
        output.collect(this.studentText, this.studyWritable);
    }
}