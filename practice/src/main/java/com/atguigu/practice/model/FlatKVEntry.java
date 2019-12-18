package com.atguigu.practice.model;


import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FlatKVEntry {
    private final String key;

    private final HEntry hEntry;

    public FlatKVEntry(String key, HEntry hEntry) {
        this.key = key;
        this.hEntry = hEntry;
    }

    public String getKey() {
        return key;
    }

    public HEntry getHEntry() {
        return hEntry;
    }


    public static List<FlatKVEntry> flattenKVEntry(KVEntry kvEntry) {
        if(kvEntry == null || StringUtils.isBlank(kvEntry.getKey()) ||  kvEntry.getHEntries() == null || kvEntry.getHEntries().isEmpty()) {
            return Collections.emptyList();
        }

        String key = kvEntry.getKey();
        List<HEntry> hEntries = kvEntry.getHEntries();

        List<FlatKVEntry> flatKVEntryList = new ArrayList<>();
        for(HEntry hEntry : hEntries) {
            flatKVEntryList.add(new FlatKVEntry(key, hEntry));
        }

        return flatKVEntryList;

    }

    @Override
    public String toString(){
        return "FlatKVEntry:{key:" + key +
                ", hKey:" + hEntry.getHKey() +
                ", hVal:" + hEntry.getHVal() +
                ", isCounter:" + hEntry.isCounter() + "}";
    }

}
