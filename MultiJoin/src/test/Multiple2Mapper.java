package test;



import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * input :
 * phone,description
 * 
 * output:
 * <key,value>  --> <[phone],[1,description]>
 * @author fansy
 *
 */
public class Multiple2Mapper extends Mapper<LongWritable,Text,Text,FlagStringDataType>{
	private  Logger log = LoggerFactory.getLogger(Multiple2Mapper.class);
	private String delimiter=null; // default is comma
	@Override
	public void setup(Context cxt){
		delimiter= cxt.getConfiguration().get("delimiter", ",");
		log.info("This is the begin of Multiple2Mapper");
	} 
	
	@Override
	public void map(LongWritable key,Text value,Context cxt) throws IOException,InterruptedException{
		String[] values= value.toString().split(delimiter);
		if(values.length!=2){
			return;
		}
		log.info("key-->"+values[0]+"=========value-->"+"[1,"+values[1]+"]");
		cxt.write(new Text(values[0]), new FlagStringDataType(1,values[1]));
	}
}
