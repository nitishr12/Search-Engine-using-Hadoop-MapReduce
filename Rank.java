package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;

public class Rank extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( Rank.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new Rank(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " Ranker ");
      job.setJarByClass( this .getClass());
	  //Get the search output file as input
      FileInputFormat.addInputPaths(job,  args[0]);
	  //Get the path of the Output folder from the console
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( DoubleWritable .class);
      job.setOutputValueClass(Text .class );
      job.setMapOutputKeyClass( DoubleWritable .class);
      job.setMapOutputValueClass(Text .class );
	  //Class to sort in descending order
      job.setSortComparatorClass(DescendingComparator.class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,DoubleWritable , Text > {

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

        String line  = lineText.toString();
        Text currentWord  = new Text();

		//Split file name from sccumulated score using tabspace
    	String wordsInLine[]=line.split("\t");
  		String fileName=wordsInLine[0];
 		double score=Double.parseDouble(wordsInLine[1]);
 		currentWord=new Text(fileName);
		//Write the fileName and score of the words found in files to the reducer
 		context.write(new DoubleWritable(score),currentWord);
  		
      }
   }

   public static class Reduce extends Reducer<DoubleWritable ,Text , Text ,  DoubleWritable > {

	   public void reduce( DoubleWritable words,  Iterable<Text> counts,  Context context)
         throws IOException,  InterruptedException {
			 //Write the filename and score in descending order
			 for ( Text t  : counts) {
				 context.write(t,  words);
			 }
      }
   }
   public static class DescendingComparator extends WritableComparator{
	   //Overrides the compare function to sort in the descending order of the score
	   @Override
	   public int compare(byte[]firstArray, int x1, int y1, byte[]secondArray, int x2, int y2){
		   double x=WritableComparator.readDouble(firstArray,x1);
		   double y=WritableComparator.readDouble(secondArray,x2);
		   if(x>y)
			   return -1;
		   else if (x<y)
			   return 1;
		   return 0;
	   }
   }
}
