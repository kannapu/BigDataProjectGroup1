import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Description: MapReduce job to get the total generation of electricity 
 * @author GROUP 1
 *
 */

public class Generation {
	/** method for mapper to give key and value*/
	public static class mapper extends Mapper<Object, Text ,IntWritable, DoubleWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			/**storing each line of data from input file in oneLine */
			String oneLine = value.toString();
			/** spliting the readed line using "," delimeter and storing in the string arrary oneArray */
			String[] oneArray = oneLine.split(",");
			/** storing in year and generation */
			int year = Integer.parseInt(oneArray[1]);
			Double generation = Double.parseDouble(oneArray[3]);
			while(value!=null){
				IntWritable keyto = new IntWritable(year); 
				context.write(keyto,new DoubleWritable(generation));
			}			
	   }
	}
	
	public static class reducer extends Reducer<IntWritable,DoubleWritable, IntWritable, DoubleWritable>{
		public void reduce(IntWritable key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
			double TotalGeneration =0;
			/** loop to read each value for particular key */
			for(DoubleWritable v : values){
				TotalGeneration=TotalGeneration + v.get();
			}	
				context.write(key, new DoubleWritable(TotalGeneration));
		}
	}
	public static void main(String[] args) throws Exception {
		/** creating a configuration object */
		Configuration conf = new Configuration();	
		/** creating a job to run the mapreducer job */
		Job jobs = Job.getInstance(conf, "TotalConsumption");
		jobs.setJarByClass(Generation.class);
		/** setting mapper and reducer class */
		jobs.setMapperClass(mapper.class);		
		jobs.setReducerClass(reducer.class);
		/** specifying the mapper output key and value datatype */
		jobs.setMapOutputKeyClass(IntWritable.class);
		jobs.setMapOutputValueClass(DoubleWritable.class);
		/** specifying the reducer output key and value datatype */
		jobs.setOutputKeyClass(IntWritable.class);
		jobs.setOutputValueClass(DoubleWritable.class);
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(jobs, new Path(args[0]));
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(jobs, new Path(args[1]));
		System.exit(jobs.waitForCompletion(true) ? 0 : 1);

	}

}
