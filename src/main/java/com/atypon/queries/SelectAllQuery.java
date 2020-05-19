package com.atypon.queries;

public class SelectAllQuery {
    private String datasetName;
    private String tableName;

    public SelectAllQuery(String tableName, String datasetName) {
        this.tableName = tableName;
        this.datasetName = datasetName;
    }

    public String getQuery() {
        return "select TO_JSON_STRING(t) from " + datasetName + "." + tableName +" ASS t";
    }

}
