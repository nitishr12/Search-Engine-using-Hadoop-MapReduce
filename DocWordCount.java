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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class DocWordCount extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( DocWordCount.class);

   public static void main( String[] args) throws  Exception {
	   //Call the DocWordCount class
      int res  = ToolRunner .run( new DocWordCount(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " docwordcount ");
      job.setJarByClass( this .getClass());

	  //Get the path of the Input folder from the console
      FileInputFormat.addInputPaths(job,  args[0]);
	  //Get the path of the Output folder from the console
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         Text currentWord  = new Text();

		 //Get the name of the input files and append it to the word
         FileSplit fs=(FileSplit)context.getInputSplit();
         String file=fs.getPath().getName();
         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
            currentWord  = new Text(word.toLowerCase()+"#####"+file);
			//sending the output of mapper to the reducer class
            context.write(currentWord,one);
         }
      }
   }

   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
		 //Get the count of each word
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
		 //Concatenate the output of mapper with the count
         context.write(word,  new IntWritable(sum));
      }
   }
}
