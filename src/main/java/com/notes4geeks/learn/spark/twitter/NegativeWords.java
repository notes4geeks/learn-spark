package com.notes4geeks.learn.spark.twitter;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Set;
import java.util.HashSet;

public class NegativeWords implements Serializable
{
    private Set<String> negativeWords;
    private static NegativeWords INSTANCE;

    private NegativeWords()
    {
        this.negativeWords = new HashSet<String>();
        BufferedReader rd = null;
        try
        {
            rd = new BufferedReader(
                new InputStreamReader(
                    this.getClass().getResourceAsStream("/negativeWords.txt")));
            String line;
            while ((line = rd.readLine()) != null)
                this.negativeWords.add(line);
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

    private static NegativeWords getInstance()
    {
        if (INSTANCE == null)
            INSTANCE = new NegativeWords();
        return INSTANCE;
    }

    public static Set<String> getWords()
    {
        return getInstance().negativeWords;
    }
}
