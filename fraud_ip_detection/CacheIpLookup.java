import java.io.*;
import java.util.*;

interface IIPScanner{
	boolean isFraudIP(String ip);
}

public class CacheIpLookup implements IIPScanner, Serializable{
	private Set<String> fraudIPList = new HashSet<>();

	public CacheIpLookup(){
		this.fraudIPList.add("23");
		this.fraudIPList.add("123");
		this.fraudIPList.add("56");
		this.fraudIPList.add("33");
		this.fraudIPList.add("34");
		this.fraudIPList.add("87");
		this.fraudIPList.add("125");
		this.fraudIPList.add("90");
		this.fraudIPList.add("24");
		this.fraudIPList.add("65");
		this.fraudIPList.add("76");
		this.fraudIPList.add("74");
		this.fraudIPList.add("29");
		this.fraudIPList.add("187");
		this.fraudIPList.add("210");
	}

	@Override
	public boolean isFraudIP(String ip){
		return fraudIPList.contains(ip);
	}
}