package test;

import java.io.IOException;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MultipleReducer extends Reducer<Text,FlagStringDataType,Text,NullWritable>{
	private  Logger log = LoggerFactory.getLogger(MultipleReducer.class);
	private String delimiter=null; // default is comma
	@Override
	public void setup(Context cxt){
		delimiter= cxt.getConfiguration().get("delimiter", ",");
	} 
	@Override
	public void reduce(Text key, Iterable<FlagStringDataType> values,Context cxt) throws IOException,InterruptedException{
		log.info("================");
		log.info("         =======");
		log.info("              ==");
		String[] value= new String[3];
		value[2]=key.toString();
		for(FlagStringDataType v:values){
			int index= v.getFlag();
			log.info("index:"+index+"-->value:"+v.get());
			value[index]= v.get();
		}
		log.info("              ==");
		log.info("         =======");
		log.info("================");
		cxt.write(new Text(value[2]+delimiter+value[0]+delimiter+value[1]),NullWritable.get());
	}
}
