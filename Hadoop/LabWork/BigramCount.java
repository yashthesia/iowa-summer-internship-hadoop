import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BigramCount {

	public static void main(String[] args) throws Exception {
		
		// Set the number of reducers
		int reduce_tasks = 8;

		// Get system configuration
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: BigramCount <in> <out>");
			System.exit(2);
		}

		// Create a Hadoop Job
		Job job_one = Job.getInstance(conf, "bigram count");

		// Attach the job to this Class
		job_one.setJarByClass(BigramCount.class);

		// Number of reducers
		job_one.setNumReduceTasks(reduce_tasks);

		// Set the 1st round of Map class to mapOne
		job_one.setMapperClass(mapOne.class);

		// Set the 1st round of Reducer class to reduceOne
		job_one.setReducerClass(reduceOne.class);

		// Set the data type of Output Key from the 1st round Map
		job_one.setMapOutputKeyClass(Text.class);

		// Set the data type of Output Value from the 1st round Map
		job_one.setMapOutputValueClass(IntWritable.class);

		// Set the data type of Output Key from the 1st round Reducer
		job_one.setOutputKeyClass(Text.class);

		// Set the data type of Output Value from the 1st round Reducer
		job_one.setOutputValueClass(IntWritable.class);

		// Set how the input is split
		// TextInputFormat.class splits the data per line
		job_one.setInputFormatClass(TextInputFormat.class);

		// Set the Output format class
		job_one.setOutputFormatClass(TextOutputFormat.class);

		// Set the Input path of 1sr round
		FileInputFormat.addInputPath(job_one, new Path(otherArgs[0]));

		// Set the Output path of 1st round
		FileOutputFormat.setOutputPath(job_one, new Path("/user/xuteng/lab2/exp2/temp"));
		
		job_one.waitForCompletion(true);
		//System.exit(job_one.waitForCompletion(true) ? 0 : 1);
		///////////////////////////////////////////////////////////////////////////////
		
                // Create job for round 2
                // The output of the previous job can be passed as the input to the next
                
                Job job_two = Job.getInstance(conf, "Bigram Count Program Round Two");
                job_two.setJarByClass(BigramCount.class);
                
                // Providing the number of reducers for the second round
                reduce_tasks = 1;
                job_two.setNumReduceTasks(reduce_tasks);

                // Should be match with the output data type of 2nd round of Mapper and Reducer
                job_two.setMapOutputKeyClass(IntWritable.class);
                job_two.setMapOutputValueClass(Text.class);
                
                // Set the data type of Output key Value from the 2nd round Reducer
                job_two.setOutputKeyClass(IntWritable.class);
                job_two.setOutputValueClass(Text.class);

                // Set the Mapper, Combiner and Reducer for 2nd round
                job_two.setMapperClass(mapTwo.class);
                job_two.setCombinerClass(reduceTwo.class);
                job_two.setReducerClass(reduceTwo.class);
                
                // Input and output format class
                job_two.setInputFormatClass(TextInputFormat.class);
                job_two.setOutputFormatClass(TextOutputFormat.class);
                
                // The output of 1st round set as the input of the 2nd computation
                FileInputFormat.addInputPath(job_two, new Path("/user/xuteng/lab2/exp2/temp"));
                FileOutputFormat.setOutputPath(job_two, new Path(otherArgs[1]));

		// Run the job
		System.exit(job_two.waitForCompletion(true) ? 0 : 1);
		

	}

	// The 1st round Mapper
	// The input to the map method would be a LongWritable key and Text value
	// Notice the class declaration is done with LongWritable key and Text value
	// The TextInputFormat splits the data line by line.
	// The key for TextInputFormat is nothing but the line number and hence can be ignored
	// The value for the TextInputFormat is a line of text from the input
	public static class mapOne extends Mapper<LongWritable, Text, Text, IntWritable> {

		// All the output values are the same which is "one", so set it as static
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		// The map method
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// The TextInputFormat splits the data line by line.
			// Convert the input line from Text to string
			String line = value.toString();
		
			// Convert all upper case letters to lower case first
			line = line.toLowerCase();
			
			// Replace any other useless punctuation marks with space, 
			// besides of "." "!" "?" which marks the end of a sentence
			line = line.replaceAll("[^a-z 0-9.?!]", " ");
			// Split the input line into a array of sentences by "." or "!" or "?"
			String[] arrayLine = line.split("\\.|\\?|\\!");
			
			// Variables for the first and second word of a bigram
			String prev = null;
			String curr = null;
			
			
			for (String sepLine:arrayLine) {
				// Tokenize to get the individual words from each sentence 
				StringTokenizer tokens = new StringTokenizer(sepLine);
				
				// Check if current sentence has more than one word
				if (tokens.hasMoreTokens()) {
					prev = tokens.nextToken();
				} else {
					continue;
				}
				
				// Exact all bigrams from current sentence
				while (tokens.hasMoreTokens()) {
					curr = tokens.nextToken();
					
					// Write bigram in Text word
					word.set(prev + " " + curr);
					context.write(word, one);
					
					prev = curr;		
			    }
			}		
		}
	}

	// The 1st round Reducer
	// The key is Text and must match the data type of the output key of the map method
	// The value is IntWritable and also must match the data type of the output value of the map method
	public static class reduceOne extends Reducer<Text, IntWritable, Text, IntWritable> {

		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			// Store frequency of each bigram
			int sum = 0;
			
			// Calculate the frequency
			for (IntWritable val : values) {
				sum += val.get();
			}

			context.write(key, new IntWritable(sum));
		}

	}
	
	// The 2nd round mapper
	// The input to the map method would be a LongWritable key and Text value
	// The TextInputFormat splits the data line by line.
	// The key for TextInputFormat is nothing but the line number and hence can be ignored
	// The value for the TextInputFormat is a line of text from the input
        public static class mapTwo extends Mapper<LongWritable, Text, IntWritable, Text> {
        	
        	public void map(LongWritable key, Text value, Context context)
        			throws IOException, InterruptedException {
        		
        		// Since we need to sort all (parts) of the frequencies in 2nd round reducer (combiner)
        		// and the input format is set to be a line of text from the input
        		// So we need to set the same key to all the values
        		// Then reducer (combiner) can receive a list of values and perform sort function
        		context.write(new IntWritable(1), value);
        		
        		} 
        }
        
        
        // The 2nd round combiner and reducer
        public static class reduceTwo extends Reducer<IntWritable, Text, IntWritable, Text> {
                public void reduce(IntWritable key, Iterable<Text> values, Context context) 
                        throws IOException, InterruptedException {

                	// Use HashMap to store 10 most frequently occurring bigrams and its corresponding frequency 
                    Map<String, Integer> bigMap = new HashMap<String, Integer>();
                    
                    // Frequency
            		int tmpVal;
            		// Bigram
            		String tmpText = null;
                    
                    for (Text bigTmp:values) {
                    	// Convert from Text to String
                    	String lineTwo = bigTmp.toString();
                    	
                    	// Split each unit (format like "i will 1087", "is the 632") by any number of consecutive spaces
                		String[] dataTmp = lineTwo.split("\\s+");
                		
                		tmpVal = Integer.parseInt(dataTmp[2]);
                		tmpText = dataTmp[0] + " " + dataTmp[1];
                		
                		// Find the minimum frequency in the current TOP 10 records
                		if (bigMap.size() < 10) { // Push first 10 bigrams and their frequencies in HashMap
                			bigMap.put(tmpText, tmpVal);
                		} else {                  // Update the minimum value of current TOP 10 bigrams
                			Map.Entry<String, Integer> minOfTop = null;
                			for (Map.Entry<String, Integer> mapVal:bigMap.entrySet()) {
                				if (minOfTop == null) {
                					minOfTop = mapVal;  // Set the first entry in HashMap as the temporary minimum value
                				} else {
                					if (mapVal.getValue() < minOfTop.getValue()) {
                						minOfTop = mapVal;
                					}
                				}
                			}
                			
                			// Compare the minimum value in HashMap with the current bigram frequency
                			// If current one is larger than the minimum one, then update HashMap
                			// Otherwise, check next bigram from input
                			if (tmpVal > minOfTop.getValue()) {
                				bigMap.remove(minOfTop.getKey());
                				bigMap.put(tmpText, tmpVal);
                			}
                		}
                    }
                    
                    // Store output bigram and frequency 
                    String resTmp = null;
                    
                    for (Map.Entry<String, Integer> tmpWrite: bigMap.entrySet()) {
                    	resTmp = tmpWrite.getKey() + " " + tmpWrite.getValue();
                    	context.write(key, new Text(resTmp));
                    }
                }
        }
        
}
