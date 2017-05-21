import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//-------------------------------------------------------------------------------------------------------------

public class Activity2b {
	
	static String mainStr = "";
	static int count = 0;
	
//--------------------------------------------------------------------------------------------------------------
	
	public static class StripesMapper extends Mapper<LongWritable,Text,Text,MapWrite> {
	    HashMap<String, Integer> hMap = new HashMap<String, Integer>();
	    Text word = new Text();
	    
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	String[] lines = value.toString().split("\n");
	    	for(String line : lines){
	    		String[] tokens = line.split("\\s+");
	    		if(tokens.length > 1){
	    			for(int i=0; i<tokens.length-1;i++){
	    				word.set(tokens[i]);
	    				hMap.clear();
	    				for(int j=i+1;j<tokens.length;j++){
	    					if(i==j)
	    						continue;
	    					String neighbor = tokens[j];
	    					if(hMap.containsKey(neighbor)){
	    						hMap.put(neighbor, hMap.get(neighbor)+1);
	    					}else{
	    						hMap.put(neighbor, 1);
	    					}
	    				}
	    				
	    				MapWrite MapWrite = new MapWrite();
	    				for (Map.Entry<String,Integer> entry : hMap.entrySet()) {
	    				    if(null != entry.getKey() && null != entry.getValue()){
	    				       MapWrite.put(new Text(entry.getKey()),new IntWritable(entry.getValue()));
	    				    }
	    				}
	    				
	    				context.write(word, MapWrite);
	    				
	    			}
	    		}
	    	}
	    }
	}	
	
	
//---------------------------------------------------------------------------------------------	
	
	public static class StripesReducer extends Reducer<Text, MapWrite, Text, MapWrite> {
	    MapWrite finalMap = new MapWrite();

	    public void reduce(Text key, Iterable<MapWrite> values, Context context) throws IOException, InterruptedException {
	        finalMap.clear();
	        for (MapWrite value : values) {
	            sum(value);
	        }		
	        
	        for(Map.Entry<Writable, Writable> entry: finalMap.entrySet()){
	        	Text tempKey = (Text) entry.getKey();
	        	IntWritable tempVal = (IntWritable) entry.getValue();
	        	mainStr += "<"+tempKey.toString()+":"+tempVal.get()+"> ; ";
	        }
	        context.write(key, finalMap);
	    }


	    public void sum(MapWrite MapWrite) {
	        Set<Writable> keys = MapWrite.keySet();
	        for (Writable key : keys) {
	            IntWritable fromCount = (IntWritable) MapWrite.get(key);
	            if (finalMap.containsKey(key)) {
	                IntWritable count = (IntWritable) finalMap.get(key);
	                count.set(count.get() + fromCount.get());
	            } else {
	                finalMap.put(key, fromCount);
	            }
	        }
	    }
	}
	
	
//-----------------------------------------------------------------------------------
	
	public static class MapWrite extends MapWritable{ 
		public String toString() {
			String returnString = mainStr;
			mainStr = "";
			return returnString;
	    }
	}
	
//-----------------------------------------------------------------------------------	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "stripes co-occurence");
        job.setJarByClass(Activity2b.class);
        job.setMapperClass(StripesMapper.class);
        job.setReducerClass(StripesReducer.class);
        job.setCombinerClass(StripesReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWrite.class);
		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWrite.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
	  }

}

//-----------------------------------------------------------------------------------	