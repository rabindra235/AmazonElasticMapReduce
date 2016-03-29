import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;

public class Map extends Mapper<LongWritable, Text, Text, Text> {
   // private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String pattern= "^[a-z][a-z0-9]*$";
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        InputSplit splitInput = context.getInputSplit();
        String fileName = ((FileSplit)splitInput).getPath().getName();

    	String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            String stringWord = word.toString().toLowerCase();
            if (stringWord.matches(pattern))
            {
                context.write(new Text(stringWord), new Text(fileName));
                //context.write(new Text(stringWord), one);
            }
            
        }
    }
}
