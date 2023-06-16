import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class WordCount_StopWords {

  // Mapper
  // takes the text, tokenize the words, process the stopwords file, check the tokenized words against the stopwords list
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();


    private Configuration conf;
    private BufferedReader fis;
    private Set<String> patternsToSkip = new HashSet<String>();

    static enum CountersEnum { INPUT_WORDS }

    // setup method to check for stopwords file and making a call to parseStopWordFile method
    public void setup(Context context) throws IOException, InterruptedException {
      
      conf = context.getConfiguration();
      if (conf.getBoolean("wordcount.skip.patterns", false)) {
        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
        for (URI patternsURI : patternsURIs) {
          Path patternsPath = new Path(patternsURI.getPath());
          String patternsFileName = patternsPath.getName().toString();
          parseStopWordFile(patternsFileName);
        }
      }
    }
  


    // parseStopWordFile method to store all stopwords in file and storing in patternsToSkip variable
    private void parseStopWordFile(String fileName) {
      try {
        fis = new BufferedReader(new FileReader(fileName));
        String pattern = null;
        while ((pattern = fis.readLine()) != null) {
          patternsToSkip.add(pattern.toLowerCase());
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '"
            + StringUtils.stringifyException(ioe));
      }
    }




    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      String line = value.toString().toLowerCase();       // converts the line to lowercase and saved as a string variable
      StringTokenizer itr = new StringTokenizer(line, "!\"#$%&()*+,-./:;<=>?@[]^_`{|}~ ");    // remove possible punctuations & tokenize the line into words
      while (itr.hasMoreTokens()) {                       // check if not empty
        String token = itr.nextToken();                   // store token in a variable
        if (!patternsToSkip.contains(token)) {            // check if token is not in patternsToSkip list
          word.set(token);                      
          context.write(word, one);
        }
      }
    }
  }

  // Reducer
  // counts the number of times the word is in data and writes the word and sum as key value pair
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    // Check for optional parameter. The program has to work even if the stopwords file has not been passed
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
      System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "word count");
     job.setJar("WordCount_StopWords.jar");
    job.setJarByClass(WordCount_StopWords.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    // verify the right parameter '-skip' is passed. Based on the parameter 
    // set config boolean paramter
    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
      if ("-skip".equals(remainingArgs[i])) {
        job.addCacheFile(new Path(remainingArgs[++i]).toUri());
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
      } else {
        otherArgs.add(remainingArgs[i]);
      }
    }


    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
    job.waitForCompletion(true);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}