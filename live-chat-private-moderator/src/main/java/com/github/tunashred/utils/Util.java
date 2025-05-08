package com.github.tunashred.utils;

import lombok.extern.log4j.Log4j2;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

@Log4j2
public class Util {
    public static Properties loadProperties(List<String> propertyFilePaths) {
        if (propertyFilePaths == null || propertyFilePaths.isEmpty()) {
            log.warn("No property files were provided");
            return null;
        }

        Properties properties = new Properties();
        for (String filePath : propertyFilePaths) {
            try (InputStream inputStream = new FileInputStream(filePath)) {
                properties.load(inputStream);
                log.trace("Loaded properties from file: {}", filePath);
            } catch (IOException e) {
                log.error("Unable to load properties from: {}", filePath, e);
            }
        }

        if (properties.isEmpty()) {
            log.warn("All the properties files were empty");
            return null;
        }
        return properties;
    }
}
