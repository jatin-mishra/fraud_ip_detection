import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyReader{

	private Properties property = null;
	
	public PropertyReader(){

		InputStream fin = null;
		try{
			this.property = new Properties();

			// fin = new InputStream("filename.properties")
			fin = this.getClass().getResourceAsStream("/streaming.properties");
			property.load(fin);
		}catch(Exception e){
			e.printStackTrace();
		}		
	}


	public String getPropertyValue(String key){
		return this.property.getProperty(key);
	}
}