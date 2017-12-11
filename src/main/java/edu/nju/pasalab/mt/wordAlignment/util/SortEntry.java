package edu.nju.pasalab.mt.wordAlignment.util;

/**
 * Created by YWJ on 2016.11.12.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class SortEntry implements Comparable<SortEntry> {
    public long count;
    public String word;
    public SortEntry(long count, String word) {
        super();
        this.count = count;
        this.word = word;
    }
    @Override
    public int compareTo(SortEntry cpt) {
        if(this.count > cpt.count)
            return -1 ;
        else if (this.count < cpt.count)
            return 1;
        return 0;
    }
}
