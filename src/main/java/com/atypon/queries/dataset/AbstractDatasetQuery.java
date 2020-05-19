package com.atypon.queries.dataset;

import com.atypon.data.adDataset.RawEvent;

public abstract class AbstractDatasetQuery {
    protected RawEvent rawEvent;
    protected String datasetName;

    public AbstractDatasetQuery (RawEvent rawEvent,String datasetName){
        this.rawEvent = rawEvent;
    }

     public static AbstractDatasetQuery getDatasetQueryInstance(String dataSetName , RawEvent rawEvent){
         AbstractDatasetQuery abstractDatasetQuery = null ;
         switch (dataSetName){
             case "ad" :{
                 abstractDatasetQuery = new AdDataSetQuery(rawEvent , dataSetName);
                break;
             }
         }
         return abstractDatasetQuery;
     }

     public abstract String getQuery();

}
