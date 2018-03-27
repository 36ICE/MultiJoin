package test;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * input :
 * username,phone
 * 
 * output:
 * <key,value>  --> <[phone],[0,username]>
 * @author fansy
 *
 */
public class Multiple1Mapper extends Mapper<LongWritable,Text,Text,FlagStringDataType>{
	private  Logger log = LoggerFactory.getLogger(Multiple1Mapper.class);
	private String delimiter=null; // default is comma
	@Override
	public void setup(Context cxt){
		delimiter= cxt.getConfiguration().get("delimiter", ",");
		log.info("This is the begin of Multiple1Mapper");
	} 
	
	@Override
	public void map(LongWritable key,Text value,Context cxt) throws IOException,InterruptedException{
		String info= new String(value.getBytes(),"UTF-8");
		String[] values = info.split(delimiter);
		if(values.length!=2){
			return;
		}
		log.info("key-->"+values[1]+"=========value-->"+"[0,"+values[0]+"]");
		cxt.write(new Text(values[1]), new FlagStringDataType(0,values[0]));
	}
}
