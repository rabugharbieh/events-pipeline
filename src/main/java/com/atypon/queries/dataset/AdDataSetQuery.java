package com.atypon.queries.dataset;

import com.atypon.data.adDataset.RawEvent;
import com.atypon.queries.dataset.AbstractDatasetQuery;
import org.apache.kafka.common.protocol.types.Field;

public class AdDataSetQuery extends AbstractDatasetQuery {
    private String tableName;

    public AdDataSetQuery(RawEvent rawEvent, String datasetName) {
        super(rawEvent, datasetName);
        tableName = datasetName + "." + rawEvent.getPublisherCode() + "_Ad_generated";
    }

    @Override
    public String getQuery() {
        return "CREATE OR REPLACE TABLE " + tableName + " (" +
                "    ad String" +
                "    ,adTitle String" +
                "    ,clicks INT64" +
                "    ,countryDescription String" +
                "    ,eventID String" +
                "    ,eventType String" +
                "    ,impressions INT64" +
                "    ,placeholder String" +
                "    ,product String" +
                "    ,publisher String" +
                "    ,publisherCode String" +
                "    ,regionCustomerNumber ARRAY<String>" +
                "    ,regionDescription ARRAY<String>" +
                "    ,sessionID String" +
                "    ,siteCode String" +
                "    ,smartGroupDescription ARRAY<String>" +
                "    ,source String" +
                "    ,subsessionID INT64" +
                "    ,timestamp Timestamp" +
                "    ,uniqueImpression Boolean" +
                "    ,webcrawlerDescription ARRAY<String>" +
                "    ,datasource String" +
                "    ,eventDate Date NOT NULL" +
                ") PARTITION BY eventDate " +
                "as(" +
                "WITH akka_base AS (" +
                "select adID, '' as adTitle, if(" + rawEvent.getEventType() + "='AdClick',1,null) as clicks, '' as countryDescription, eventID,if(" + rawEvent.getEventType() + "='AdClick' or " + rawEvent.getEventType() + "='AdImpression','Ad',null) as eventType, if(" + rawEvent.getEventType() + "='AdImpression',1,null) as impression,'' as sessionID, placeholderID, productID, contentID,ARRAY_CONCAT(identityID,[0]) as identityID, publisherCode, siteCode, subsessionID, timestamp,uniqueImpression from akka_events." + rawEvent.getPublisherCode() + "_ad_akka where 42 not in UNNEST(identityID)" +
                ")," +
                "akka_dim_ad AS (" +
                "select  base.adID, adTitle,clicks, countryDescription, eventID, eventType, impression, sessionID, placeholderID, productID, contentID, identityID, publisherCode, siteCode, subsessionID, timestamp,uniqueImpression, ad_dim.description as ad from akka_base base  " +
                "left join akka_events." + rawEvent.getPublisherCode() + "_dim_ad ad_dim on base.adID = ad_dim.adID" +
                ")," +
                "akka_dim_placeholder AS (" +
                "select adID, adTitle,clicks, countryDescription, eventID, eventType, impression, sessionID, dim_ad.placeholderID, productID, contentID,  identityID, publisherCode, siteCode, subsessionID, timestamp,uniqueImpression, ad , dim_placeholder.description as placeholder from akka_dim_ad dim_ad" +
                "left join akka_events." + rawEvent.getPublisherCode() + "_ad_placeholder dim_placeholder on dim_placeholder.placeholderID = dim_ad.placeholderID" +
                ")," +
                "akka_ad_product AS (" +
                "select adID,adTitle,clicks, countryDescription, eventID,eventType, impression, sessionID, placeholderID, akka_dim_placeholder.productID, contentID,identityID, publisherCode, siteCode, subsessionID, timestamp,uniqueImpression,ad,placeholder, dim_ad_product.description as product  from akka_dim_placeholder" +
                " left join  akka_events." + rawEvent.getPublisherCode() + "_ad_product dim_ad_product on dim_ad_product.productID = akka_dim_placeholder.productID" +
                ")," +
                "regions_user1 AS (" +
                "select  eventID, adID,adTitle,clicks, eventType, impression, sessionID,contentID, publisherCode, siteCode, subsessionID,  timestamp,uniqueImpression,ad,placeholder,  product , " +
                "ARRAY_AGG(region.regionCode ignore nulls) as regionCustomerNumber ," +
                "ARRAY_AGG(region.regionName ignore nulls) as regionDescription," +
                "ARRAY_AGG(identity) as identity," +
                "STRING_AGG((case when region.subdivisionid =0 then region.regionName else NULL END),'') as countryDescription, " +
                "null as publisher, null as smartGroupDescription , null as webcrawlerDescription from akka_ad_product," +
                "UNNEST(identityID) as identity" +
                "left join akka_events.fum_dim_region_user region on region.regionID=identity" +
                "group by adID,adTitle,clicks,eventID,eventType, impression, sessionID,contentID, publisherCode, siteCode, subsessionID,  timestamp,uniqueImpression,ad,placeholder,product" +
                ")," +
                "akka_dim_user AS (" +
                "select adID,adTitle,clicks, countryDescription, eventID,eventType, impression, sessionID, contentID as ourContentID, publisherCode, siteCode, subsessionID,  timestamp,uniqueImpression,ad,placeholder,  product ," +
                "STRING_AGG((case when user.typeID=527 then user.description else NULL END),'') as publisher," +
                "ARRAY_AGG(if(user.typeID=534,user.description,null) ignore nulls) as smartGroupDescription," +
                "ARRAY_AGG(if(user.typeID=516,user.description,null) ignore nulls) as webcrawlerDescription " +
                "from regions_user1," +
                "unnest(identity) as identity" +
                "left join  akka_events." + rawEvent.getPublisherCode() + "_dim_user user on user.userID = identity " +
                "group by eventID,adID,adTitle,clicks, countryDescription, eventID,eventType, impression, sessionID,  ourContentID,  publisherCode, siteCode, subsessionID,timestamp,uniqueImpression,ad,placeholder,  product" +
                ")," +
                "semi_final AS (select dim.ad,dim.adTitle,dim.clicks,dim.countryDescription,dim.eventID,dim.eventType,dim.impression,dim.placeholder,dim.product,dim.publisher,dim.publisherCode,regionCustomerNumber,regionDescription,dim.sessionID,dim.siteCode,dim.smartGroupDescription,null as source, dim.subsessionID,dim.timestamp,dim.uniqueImpression,dim.webcrawlerDescription, DATE(dim.timestamp,'America/Los_Angeles') as eventDate,ourContentID from regions_user1 users left join akka_dim_user dim on dim.eventID=users.eventID" +
                ")," +
                "final AS (" +
                "select ourContentID,eventID , dim_article.doi source from semi_final semi left join akka_events." + rawEvent.getPublisherCode() + "_dim_article dim_article on ourContentID=dim_article.articleID" +
                ")," +
                "done AS (" +
                "select dim.ad,dim.adTitle,dim.clicks,dim.countryDescription,dim.eventID,dim.eventType,dim.impression,dim.placeholder,dim.product,dim.publisher,dim.publisherCode,regionCustomerNumber,regionDescription,dim.sessionID,dim.siteCode,smartGroupDescription as smartGroupDescription ,users.source, dim.subsessionID,dim.timestamp,dim.uniqueImpression,dim.webcrawlerDescription, '' as datasource, DATE(dim.timestamp,'America/Los_Angeles') as eventDate from semi_final dim" +
                "left join final  users on dim.eventID=users.eventID" +
                "" +
                ")" +
                "select * from done)";
    }
}
