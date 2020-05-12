package com.packt.Kafka.utils;

import java.io.FileNotFoundException;
import java.io.Exception;
import java.io.InputStream;
import java.util.Properties;


public class PropertyReader{

	private Properties prop = null;

	public PropertyReader(){
		InputStream is = null;

		try{
			this.prop = new Properties();
			is = this.getClass().getResourceAsStream("./streaming.properties");
			prop.load(is);
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public String getPropertyValue(String key){
		return this.prop.getProperty(key);
	}
}