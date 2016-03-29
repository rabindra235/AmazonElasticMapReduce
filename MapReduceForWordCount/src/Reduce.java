import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class Reduce extends Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
        /*int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));*/
    	
    	int sum = 0;
    	int totalCount =0;
    	boolean bolCheck =false;
    	String fileName = "";
    	String strTxtFormat = "{";
    	
    	for(Text value:values)
    	{
    		if(!bolCheck)
    		{
    			fileName = value.toString();
    			bolCheck= true;
    		}
    		if(fileName.equalsIgnoreCase(value.toString()))
    		{
    			sum = sum+1;
    			totalCount+=1; 
    		}
    		else
    		{
    			strTxtFormat+= fileName +"="+ sum+",";
    			fileName = value.toString();
    			sum=1;
    			totalCount+=1;
    			
    		}
    		//totalCount+=sum;
    	}
    	//totalCount+=sum;
    	strTxtFormat+= fileName +"="+ sum +"}\n";
    	context.write(key, new Text(strTxtFormat+"Total occurenece of "+"'"+key+"'"+":"+totalCount));
    	//context.write(key, new Text("The occurrence of"+key + "is:"+ totalCount));
    	//context.write("The occurrence of"+key+"is", new Text(":"+totalCount));
    }
}