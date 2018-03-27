package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
/**
 * input1(/multiple/user/user):
 * username,user_phone
 *  
 * input2(/multiple/phone/phone):
 *  user_phone,description 
 *  
 * output: username,user_phone,description
 * 
 * @author fansy
 *
 */
public class MultipleDriver extends Configured implements Tool{
//	private  Logger log = LoggerFactory.getLogger(MultipleDriver.class);
	
	private String input1=null;
	private String input2=null;
	private String output=null;
	private String delimiter=null;
	
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
//		conf.set("fs.defaultFS", "hdfs://node33:8020");  
//        conf.set("mapreduce.framework.name", "yarn");  
//        conf.set("yarn.resourcemanager.address", "node33:8032"); 
        
		ToolRunner.run(conf, new MultipleDriver(), args);
	}


	@Override
	public int run(String[] arg0) throws Exception {
		configureArgs(arg0);
		checkArgs();
		
		Configuration conf= getConf();
		conf.set("delimiter", delimiter);
		 @SuppressWarnings("deprecation")
		Job job = new Job(conf, "merge user and phone information ");
        job.setJarByClass(MultipleDriver.class);


        job.setReducerClass(MultipleReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlagStringDataType.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        job.setNumReduceTasks(1);
        MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class, Multiple1Mapper.class);
        MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class, Multiple2Mapper.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        
        int res = job.waitForCompletion(true) ? 0 : 1;
        return res;
	}
	


	/**
	 * check the args 
	 */
	private void checkArgs() {
		if(input1==null||"".equals(input1)){
			System.out.println("no user input...");
			printUsage();
			System.exit(-1);
		}
		if(input2==null||"".equals(input2)){
			System.out.println("no phone input...");
			printUsage();
			System.exit(-1);
		}
		if(output==null||"".equals(output)){
			System.out.println("no output...");
			printUsage();
			System.exit(-1);
		}
		if(delimiter==null||"".equals(delimiter)){
			System.out.println("no delimiter...");
			printUsage();
			System.exit(-1);
		}
	
	}


	/**
	 * configuration the args
	 * @param args
	 */
	private void configureArgs(String[] args) {
    	for(int i=0;i<args.length;i++){
    		if("-i1".equals(args[i])){
    			input1=args[++i];
    		}
    		if("-i2".equals(args[i])){
    			input2=args[++i];
    		}
    		
    		if("-o".equals(args[i])){
    			output=args[++i];
    		}
    		
    		if("-delimiter".equals(args[i])){
    			delimiter=args[++i];
    		}
    		
    	}
	}
	public static void printUsage(){
    	System.err.println("Usage:");
    	System.err.println("-i1 input \t user data path.");
    	System.err.println("-i2 input \t phone data path.");
    	System.err.println("-o output \t output data path.");
    	System.err.println("-delimiter  data delimiter , default is comma  .");
    }
}
