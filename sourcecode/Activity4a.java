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

public class Activity4a {
	
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
				for(int a=0;a<tokens.length-1;a++){ //For each word in a single line		
					for(int b=a+1;b<tokens.length;b++){
						String normalized = tokens[a];
						String neighbour = tokens[b];
						normalized = process(normalized);
						neighbour = process(neighbour);
						
						String lemma =  hMap.get(normalized);
						String lemmaN = hMap.get(neighbour);
						
						if(lemma!=null && lemmaN!=null){
							String[] lemma1 = lemma.split(",");
							String[] lemma2 = lemmaN.split(",");
							for(int i=0;i<lemma1.length;i++){
								for(int j=0;j<lemma2.length;j++){
									String wordPair = "<"+lemma1[i]+","+lemma2[j]+">";
									context.write(new Text(wordPair), new Text(location));
								}
							}	
						}else if(lemma!=null && lemmaN==null){
							String[] lemma1 = lemma.split(",");
							for(int i=0;i<lemma1.length;i++){
								String wordPair = "<"+lemma1[i]+","+neighbour+">";
								context.write(new Text(wordPair), new Text(location));
							}
						}else if(lemma==null && lemmaN!=null){
							String[] lemma2 = lemmaN.split(",");
							for(int j=0;j<lemma2.length;j++){
								String wordPair = "<"+normalized+","+lemma2[j]+">";;
								context.write(new Text(wordPair), new Text(location));	
							}
						}else{
							String wordPair = "<"+normalized+","+neighbour+">";;
							context.write(new Text(wordPair), new Text(location));
						}	
						
						
						
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
				
			String allPos = "{";
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
			for(int i=1;i<split.length;i++){
				lemmaList += split[i]+",";
			}
			hMap.put(split[0], lemmaList);   //In the hashmap we put key as the normalized word and value as its lemma/lemmas
		}
		System.out.println("#####Done reading#####");
	}
	
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		readFile();
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Activity 4a");
		job.setJarByClass(Activity4a.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
