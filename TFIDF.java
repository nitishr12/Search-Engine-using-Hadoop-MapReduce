package org.myorg;

import java.io.IOException;
import java.util.*;
import org.myorg.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class TFIDF extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TFIDF.class);
   public final static String FILE_REFERENCE="CountOfFiles";
   

   public static void main( String[] args) throws  Exception {
	   //Running Term Frequency job first to get the term frequency of all the words
	  int res=ToolRunner .run( new TermFrequency(), args);
	  //Run the TFIDF job then
      res=ToolRunner.run(new TFIDF(),args);
      //Result Code
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      
	  Job job  = Job .getInstance(getConf(), " TF IDF ");
      job.setJarByClass( this .getClass());
	   int totalFiles=0;
      //Find the total # of files for IDF
      FileSystem fileSystem=FileSystem.get(getConf());
	  //Get the path of the Input folder from the console
      FileStatus fileStatuses[]= fileSystem.listStatus(new Path(args[0]));
      for(FileStatus temp: fileStatuses){
    	  totalFiles+=1;
      }
      
      job.getConfiguration().set(FILE_REFERENCE,Double.valueOf(totalFiles)+"");

	   //Get the path of the temporary output folder for the Term Frequency from the console
      FileInputFormat.addInputPaths(job,  args[1]);
	   //Get the path of the Output folder from the console
      FileOutputFormat.setOutputPath(job,  new Path(args[2]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( DoubleWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
      
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         Text currentWord  = new Text();

         	if(line.isEmpty()==false){
				//Split the word from the filename by using the string "#####"
         		String wordsInLine[]=line.split("#####");
         		String word=wordsInLine[0];
				//Split the file name from the term frequency by using tabspace
         		String wordsInLine2[]=wordsInLine[1].split("\t");
         		String fileName=wordsInLine2[0];
         		double termFreq=Double.parseDouble(wordsInLine2[1]);
				//Concatenate the filename with "=" and term frequency
				currentWord  = new Text(fileName+"="+termFreq);
				context.write(new Text(word),currentWord);
         }
		 //sending the output of mapper to the reducer class
      }
   }

   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
	  @Override
      public void reduce( Text word,  Iterable<Text > counts,  Context context)
         throws IOException,  InterruptedException {
    	  double termFrequency=0.0;
    	  Text textWords=new Text();
    	 double countOfDocs=0.0;
    	 ArrayList<String> wordsInFile= new ArrayList<>();
		 // get the total # of files by using the reference from main class
         double numOfFiles=Double.parseDouble(String.valueOf(context.getConfiguration().get(FILE_REFERENCE)));
		 // Count the # of docs where the word is present and add the words to the arraylist
         for(Text doc: counts){
        	 countOfDocs+=1;
        	 wordsInFile.add(doc.toString());
         }
		 //Calculate the IDF value
         double idfValue=Math.log10(1.0+ (numOfFiles/countOfDocs));
		 //For every word calculate the TFIDF value
         for(String s : wordsInFile){
        	 String temp1[]=s.split("=");
        	 String fileName=temp1[0];
        	 termFrequency=Double.parseDouble(temp1[1]);
			 //Concatenate the output of mapper with the word, "#####" and the filename
        	 textWords= new Text(word.toString()+"#####"+fileName);
			 //Write the TFIDF value with the concatenated word in the output file
        	 context.write(textWords,  new DoubleWritable(termFrequency*idfValue));
         }
      }
   }
}
