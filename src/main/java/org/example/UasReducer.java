package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class UasReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable>{
    @Override
    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output,
                       Reporter reporter) throws IOException
    {
        System.out.println("------------------");
        System.out.println("Ini dari reducer");
        System.out.println("Key: " + key.toString());
        System.out.println("------------------");
        int sum = 0;
        int count = 0;

        while (values.hasNext())
        {
            LongWritable currentStudy = values.next();
            System.out.println("Isi dari values saat ini: " + currentStudy.get());
            sum += currentStudy.get();
            count += 1;
        }

        int average = sum / count;
        output.collect(key, new LongWritable(average));
    }
}