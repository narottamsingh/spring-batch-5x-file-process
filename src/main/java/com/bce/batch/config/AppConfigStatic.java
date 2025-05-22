package com.bce.batch.config;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AppConfigStatic {

    private static String name;
    private static String version;
    private static String description;

    @Autowired
    private AppConfig appConfig;

    @PostConstruct
    public void init() {
        name = appConfig.getName();
        version = appConfig.getVersion();
        description = appConfig.getDescription();
    }

    public static String getName() {
        return name;
    }

    public static String getVersion() {
        return version;
    }

    public static String getDescription() {
        return description;
    }
}
