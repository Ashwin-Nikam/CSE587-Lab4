import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Activity3 {
	
	static HashMap<String, String> hMap = new HashMap<String, String>();

	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String lines[] = value.toString().split("\n");
			for(String line: lines){
				String tabToken[] = line.split(">");
	        	String location = tabToken[0];
	        	if(!line.contains(">"))
	        		continue;
	        	location += ">";
	        	tabToken[1] = tabToken[1].trim();
	        	if(tabToken[1].equals(""))
	        		continue;
	        	String tokens[] = tabToken[1].split("\\s+");
				for(int i=0;i<tokens.length;i++){ //For each word in a single line		
					String normalized = tokens[i];
					normalized = process(normalized);
					
					String lemma = "";
					if(hMap.get(normalized)!=null){
						lemma = hMap.get(normalized);
						String lemmaList[] = lemma.split(",");
						for(int j=0;j<lemmaList.length;j++){
							//System.out.println("LEMMA "+lemmaList[j]+" "+location);
							context.write(new Text(lemmaList[j]), new Text(location));
						}
					}else{
						//System.out.println(normalized+" "+location);
						context.write(new Text(normalized), new Text(location));
					}		
				}
				
			}
			
		}
		
		public static String process(String normalized){
			normalized = normalized.replaceAll("j", "i"); //Replacing letters in the words of that line
			normalized = normalized.replaceAll("v", "u");
			normalized = normalized.replaceAll("\\?", "");
			normalized = normalized.replaceAll("\\.", "");
			normalized = normalized.replaceAll("\"", "");
			normalized = normalized.replaceAll("\t", "");
			normalized = normalized.replaceAll("\\,", "");
			normalized = normalized.replaceAll("\\:", "");
			normalized = normalized.replaceAll("\\;", "");
			normalized = normalized.replaceAll("\\(", "");
			normalized = normalized.replaceAll("\\)", "");
			normalized = normalized.replaceAll("\\!", "");
			return normalized;
		}
		
	}
	
	public static class ReducerClass extends Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text word, Iterable<Text>positions, Context context) throws IOException, InterruptedException{			
			String allPos = "{ ";
			int count = 0;
			for(Text position: positions){
				allPos += position.toString()+" "; 
				count++;
			}
			allPos += " count: "+count+"}";
			Text finalPosList = new Text(allPos);
			context.write(word, finalPosList);			
		}
		
	}
	
	public static void readFile() throws IOException{
		BufferedReader br= new BufferedReader(new FileReader("/home/hadoop/new_lemmatizer.csv"));
		System.out.println("#####About to read#####");
		String mainLine;
		while((mainLine = br.readLine())!=null){
			String[] split = mainLine.split(",");
			String lemmaList = "";
			if(split.length>2){
				for(int i=1;i<split.length;i++){
					lemmaList += split[i]+",";
				}
				hMap.put(split[0], lemmaList);
			}else
				hMap.put(split[0], split[1]);   //In the hashmap we put key as the normalized word and value as its lemma/lemmas
		}
		System.out.println("#####Done reading#####");
	}
	
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		readFile();
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Activity 3");
		job.setJarByClass(Activity3.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
