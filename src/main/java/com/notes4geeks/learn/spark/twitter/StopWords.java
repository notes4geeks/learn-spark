package com.notes4geeks.learn.spark.twitter;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;

public class StopWords implements Serializable
{
    public static final long serialVersionUID = 42L;
    private List<String> stopWords;
    private static StopWords INSTANCE;

    private StopWords()
    {
        this.stopWords = new ArrayList<String>();
        BufferedReader rd = null;
        try
        {
            rd = new BufferedReader(
                new InputStreamReader(
                    this.getClass().getResourceAsStream("/stopWords.txt")));
            String line = null;
            while ((line = rd.readLine()) != null)
                this.stopWords.add(line);
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

    private static StopWords getInstance()
    {
        if (INSTANCE == null){
        	synchronized (StopWords.class) {
        		INSTANCE = new StopWords();
			}
        }
            
        return INSTANCE;
    }

    public static List<String> getWords()
    {
        return getInstance().stopWords;
    }
}
