import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Marlon on 16/02/2018.
 */
public class Utils {

    Utils(){
        initProperties();
    }

    private static Logger logger = LoggerFactory.getLogger(Utils.class);
    private static Properties properties;

    private void initProperties(){
        properties = new Properties();
        try {
            properties.load(getClass().getResourceAsStream("/api_twitter.properties"));
        } catch (IOException ex) {
            logger.error("Error get properties from file system... ");
        }
    }

    public String getKey(String key){
        return properties.getProperty(key);
    }
}
