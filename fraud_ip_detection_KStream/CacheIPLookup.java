package com.packt.Kafka.lookup;

interface IIPScanner{
	boolean isFraudIP(String ipAddress);
}

public class CacheIPLookup implements IIPScanner{

	private Set<String> fraudIpList = new HashSet<>();

	public CacheIPLookup(){
		fraudIpList.add("136");
		fraudIpList.add("234");
		fraudIpList.add("22");
		fraudIpList.add("16");
		fraudIpList.add("78");
		fraudIpList.add("190");
		fraudIpList.add("111");
		fraudIpList.add("24");
		fraudIpList.add("15");
		fraudIpList.add("44");
		fraudIpList.add("76");
		fraudIpList.add("217");
		fraudIpList.add("163");
	}

	@Override
	public boolean isFraudIP(String ipAddress){
		return fraudIpList.contains(ipAddress);
	}
}