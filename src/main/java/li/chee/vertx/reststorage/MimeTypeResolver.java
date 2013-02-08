package li.chee.vertx.reststorage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MimeTypeResolver {

    Map<String, String> mimeTypes = new HashMap<String,String>();
    
    public MimeTypeResolver() {
        Properties props = new Properties();
        try {
            props.load(this.getClass().getClassLoader().getResourceAsStream("mime-types.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        for( Map.Entry<Object, Object> entry : props.entrySet()) {
            mimeTypes.put(((String)entry.getKey()).toLowerCase(), (String)entry.getValue());
        }        
    }
    
    public String getMimeType(String extension) {
        return mimeTypes.get(extension.toLowerCase());
    }
}
