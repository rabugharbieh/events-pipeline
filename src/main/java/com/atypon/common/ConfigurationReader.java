package com.atypon.common;


import com.atypon.exceptions.ConfigurationReaderException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationFactory;

public class ConfigurationReader {
    Configuration config = null;

    public ConfigurationReader() {
    }

    public void load() throws ConfigurationReaderException {
        try {
            ConfigurationFactory factory = new ConfigurationFactory();
            factory.setConfigurationFileName(this.getEnvConfigXMLPath());
            config = factory.getConfiguration();
        } catch (Exception exc) {
            throw new ConfigurationReaderException(exc.getMessage());
        }
    }

    public Configuration getConfiguration() throws
            ConfigurationReaderException {
        if (config == null) {
            load();
        }
        return config;
    }

    private String getEnvConfigXMLPath() {
        StringBuffer srtBuffer = new StringBuffer("configuration/");
        // System env variable is mapped to configuration/[env] folder
        srtBuffer.append(System.getProperty("env"));
        srtBuffer.append("/config.xml");
        return srtBuffer.toString();
    }
}
