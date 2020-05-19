package utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PipelineUtils {

    public static Properties getProperties(String configDir) {
        try {
            String fullDir = System.getProperties().get("user.dir") + configDir;
            File file = new File(fullDir);
            FileInputStream fileInput = new FileInputStream(file);
            Properties properties = new Properties();
            properties.loadFromXML(fileInput);
            fileInput.close();
            return properties;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
