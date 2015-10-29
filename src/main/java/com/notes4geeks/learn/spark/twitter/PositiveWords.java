package com.notes4geeks.learn.spark.twitter;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Set;
import java.util.HashSet;

public class PositiveWords implements Serializable
{
    public static final long serialVersionUID = 42L;
    private Set<String> positiveWords;
    private static PositiveWords INSTANCE;

    private PositiveWords()
    {
        this.positiveWords = new HashSet<String>();
        BufferedReader rd = null;
        try
        {
            rd = new BufferedReader(
                new InputStreamReader(
                    this.getClass().getResourceAsStream("/positiveWords.txt")));
            String line;
            while ((line = rd.readLine()) != null)
                this.positiveWords.add(line);
        }
        catch (IOException ex)
        {
            Logger.getLogger(this.getClass())
                  .error("IO error while initializing", ex);
        }
        finally
        {
            try {
                if (rd != null) rd.close();
            } catch (IOException ex) {}
        }
    }

    private static PositiveWords getInstance()
    {
        if (INSTANCE == null)
            INSTANCE = new PositiveWords();
        return INSTANCE;
    }

    public static Set<String> getWords()
    {
        return getInstance().positiveWords;
    }
}
