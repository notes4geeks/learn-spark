package com.notes4geeks.learn.spark.twitter;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

public class Properties
{
    private static final Logger LOGGER = Logger.getLogger(Properties.class);
    private static Properties INSTANCE;

    private Configuration config;

    private Properties()
    {
        try
        {
            this.config = new PropertiesConfiguration(
                this.getClass().getResource("/spark.properties"));
        }
        catch (Exception ex)
        {
            LOGGER.fatal("Could not load configuration", ex);
            LOGGER.trace(null, ex);
        }
    }

    private static Properties getInstance()
    {
        if (INSTANCE == null)
            INSTANCE = new Properties();
        return INSTANCE;
    }

    public static String getString(String key)
    {
        return getInstance().config.getString(key);
    }

    public static Integer getInt(String key)
    {
        return getInstance().config.getInt(key);
    }
}
