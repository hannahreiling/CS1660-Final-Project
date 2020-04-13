import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

//modified from WordCount code provided on 'Hadoop Project GCP Gudie'
public class InvertedIndex 
{
	//default constructor
	InvertedIndex(){}

	public static void main(String[] args) 
		throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.err.println("Usage: Inverted Index <input path> <output path>");
			System.exit(-1);
		}
		//Creating a Hadoop job and assigning a job name for identification.
		Job job = new Job();
		job.setJarByClass(InvertedIndex.class);
		job.setJobName("Inverted Index");
		//The HDFS input and output directories to be fetched from the Dataproc job submission console.
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//Proividng the mapper and reducer class names.
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		//Setting the job object with the data types of output key(Text) and value(Text).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
	}

	/*
	This is the Reducer class. It extends the Hadoop's Reducer class.
	This maps the intermediate key/value pairs we get from the mapper to a set
	of output key/value pairs, where the key is the word and value is docId:word's count.
	Here our input key is a Text and input value is a Text.
	And the ouput key is a Text and the value is a Text.
	*/
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>
	{
		/*
		Reduce method collects the output of the Mapper and combines individual counts into hashmap.
		*/
		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException
		{
			HashMap<String, Integer> hm = new HashMap<String, Integer>();
			/*
			Iterates through all the values available with a key and adds the value and its count to the 
			hashmap and gives the final result as the key and its count in each documents (docId:word(key) count)
			*/
			for (Text value : values)
			{
				String valueString = value.toString();
				String valueStringShortened = valueString.substring(valueString.lastIndexOf("/") + 1);
				if(hm.containsKey(valueStringShortened)) {
					int sum = hm.get(valueStringShortened);
					sum += 1;
					hm.put(valueStringShortened, new Integer(sum));
				} else {
					hm.put(valueStringShortened, new Integer(1));
				}
			}
			StringBuilder sb = new StringBuilder("");

			//format hashmap values into docId:word count so it's easier to print/view
			for(String temp : hm.keySet()) {
				sb.append(temp + ":" + hm.get(temp) + "\t");
			}
			sb.append("\n------------------------------------------------------------\n");
			context.write(key, new Text(sb.toString()));
		}
	}

	/*
	This is the Mapper class. It extends the Hadoop's Mapper class.
	This maps input key/value pairs to a set of intermediate (output) key/value pairs.
	Here our input key is a LongWritable and input value is a Text.
	And the output key is a Text and value is an Text.
	*/
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text word = new Text();
		Text docID = new Text();

		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException
		{
			//Reading input one line at a time and tokenizing.
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			//Getting docID (filename)
			String docIDstring = ((FileSplit)context.getInputSplit()).getPath().toString();
			String docIDstringShort = docIDstring.substring(docIDstring.lastIndexOf("/") + 1);
			docID = new Text(docIDstringShort);

			//Iterating through all the words available in that line and forming the key value pair.
			while (tokenizer.hasMoreTokens())
			{
				//preprocess word to eliminate any nonalphabet chars and make lowercase 
				//allows words like Hello and hello to be viewed as same words in reducer step
				word.set(preprocess(tokenizer.nextToken()));
				//Sending to output collector(Context) which in-turn passes the output to Reducer.
				context.write(word, docID);
			}
		}

		//make string uniform
		public String preprocess(String str) 
		{
			str = str.toLowerCase(); //make everything lowercase
			str = str.replaceAll("[^a-zA-Z]", ""); //remove any nonalphabet chars
			return str;
		}
	}
}







