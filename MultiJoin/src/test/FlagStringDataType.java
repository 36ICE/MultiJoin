package test;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.google.common.primitives.Ints;


public class FlagStringDataType implements WritableComparable<FlagStringDataType> {
	private  Logger log = LoggerFactory.getLogger(FlagStringDataType.class);
  private String value;
  private int flag;
  public FlagStringDataType() {
  }


  public FlagStringDataType(int flag,String value) {
    this.value = value;
    this.flag=flag;
  }


  public String get() {
    return value;
  }


  public void set(String value) {
    this.value = value;
  }


  @Override
  public boolean equals(Object other) {
    return other != null && getClass().equals(other.getClass()) 
    		&& ((FlagStringDataType) other).get() == value
    		&&((FlagStringDataType) other).getFlag()==flag;
  }


  @Override
  public int hashCode() {
    return Ints.hashCode(flag)+value.hashCode();
  }


  @Override
  public int compareTo(FlagStringDataType other) {
	 
    if (flag >= other.flag) {
      if (flag > other.flag) {
        return 1;
      }
    } else {
      return -1;
    }
    return value.compareTo(other.value);
  }


  @Override
  public void write(DataOutput out) throws IOException {
	log.info("in write()::"+"flag:"+flag+",vlaue:"+value);
    out.writeInt(flag);
    out.writeUTF(value);
  }


  @Override
  public void readFields(DataInput in) throws IOException {
	  log.info("in read()::"+"flag:"+flag+",vlaue:"+value);
	  flag=in.readInt();
	  value = in.readUTF();
	  log.info("in read()::"+"flag:"+flag+",vlaue:"+value);
  }


public int getFlag() {
	return flag;
}


public void setFlag(int flag) {
	this.flag = flag;
}


public String toString(){
	return flag+":"+value;
}


}
