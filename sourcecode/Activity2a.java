import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//------------------------------------------------------------------------------------------------------------------------

public class Activity2a {
	
//------------------------------------------------------------------------------------------------------------------------	

	public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
	    IntWritable occur = new IntWritable(1);
	    String word;
	    String neighbor;
	    String pair;

	   
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	String[] lines = value.toString().split("\n");
	    	for(String line : lines){
	    		String[] tokens = line.split("\\s+");
	    		if(tokens.length > 1){
	    			for(int i = 0; i<tokens.length-1;i++){
	    				word = tokens[i];
	    				for(int j=i+1 ;j<tokens.length;j++){
	    					if(i==j)
	    						continue;
	    					neighbor = tokens[j];
	    					pair = "<word:"+word+" | neighbor:"+neighbor+">";
	    					Text Pair = new Text(pair);
	    					context.write(Pair, occur);
	    				}
	    			}
	    		}
	    	} 
	    }
	}

//-----------------------------------------------------------------------------------------------------------------------	

	public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
	    IntWritable totalCount = new IntWritable();
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	        int count = 0;
	        for (IntWritable value : values) {
	             count += value.get();
	        }
	        totalCount.set(count);
	        context.write(key,totalCount);
	    }
	}
  
//----------------------------------------------------------------------------------------------------------------------  
  
  public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "pairs co-occurence");
	    job.setJarByClass(Activity2a.class);
	    job.setMapperClass(MapperClass.class);
	    job.setCombinerClass(ReducerClass.class);
	    job.setReducerClass(ReducerClass.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }  
  
}

//-----------------------------------------------------------------------------------------------------