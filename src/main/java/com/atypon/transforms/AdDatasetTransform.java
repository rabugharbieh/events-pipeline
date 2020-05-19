package com.atypon.transforms;

import com.atypon.data.adDataset.RawEvent;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.base.Throwables;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.codehaus.jackson.map.ObjectMapper;


public class AdDatasetTransform<T> extends PTransform<PCollection<FailsafeElement<T, Object>>, PCollectionTuple> {
    private TupleTag<FailsafeElement<T, Object>> successTag;
    private TupleTag<FailsafeElement<T, Object>> failureTag;

    AdDatasetTransform(TupleTag<FailsafeElement<T, Object>> successTag , TupleTag<FailsafeElement<T, Object>> failureTag ){
        this.successTag = successTag;
        this.failureTag = failureTag;
    }

    public static <T> AdDatasetTransform.Builder<T> newBuilder() {
        return new AdDatasetTransform.Builder();
    }

    public TupleTag<FailsafeElement<T, Object>> successTag() {
        return this.successTag;
    }

    public TupleTag<FailsafeElement<T, Object>> failureTag() {
        return this.failureTag;
    }

    @Override
    public PCollectionTuple expand(PCollection<FailsafeElement<T, Object>> input) {
        return (PCollectionTuple) input.apply("ProcessAdTransformer", ParDo.of(new DoFn<FailsafeElement<T, Object>, FailsafeElement<T, Object>>() {

            @ProcessElement
            public void processElement(DoFn<FailsafeElement<T, Object>, FailsafeElement<T, Object>>.ProcessContext context) {
                FailsafeElement<T, Object> element = context.element();
                String payloadStr = (String) element.getPayload();
                ObjectMapper mapper = new ObjectMapper();
                try {
                    RawEvent rawEvent = mapper.readValue(payloadStr, RawEvent.class);
                    TypedRead<TableRow> result ;
                    if (rawEvent != null) {
                        String query = getAdDatasetCreateQuery(rawEvent);
                        result= BigQueryIO.readTableRows()
                                .fromQuery(query);
//                        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
//                        String query = getAdDatasetCreateQuery(rawEvent);
//                        QueryJobConfiguration queryConfig =
//                                QueryJobConfiguration.newBuilder(query)
//                                        .setUseLegacySql(false)
//                                        .build();
//                        // Create a job ID so that we can safely retry.
//                        JobId jobId = JobId.of(UUID.randomUUID().toString());
//                        queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
//
//                        // Wait for the query to complete.
//                        queryJob = queryJob.waitFor();
//
//                        // Check for errors
//                        if (queryJob == null) {
//                            throw new RuntimeException("Job no longer exists");
//                        } else if (queryJob.getStatus().getError() != null) {
//                            // You can also look at queryJob.getStatus().getExecutionErrors() for all
//                            // errors, not just the latest one.
//                            throw new RuntimeException(queryJob.getStatus().getError().toString());
//                        }
//                        query = getAdDatasetCreateQuery(rawEvent);
//                        result = BigQueryIO.readTableRows()
//                                .fromQuery(query);
                        context.output(AdDatasetTransform.this.successTag() , FailsafeElement.of(element.getOriginalPayload(), result));
                    }


//                    Schema schema =
//                            Schema.of(
//                                    Field.of("stringField", LegacySQLTypeName.legacySQLTypeName(StandardSQLTypeName.STRING)),
//                                    Field.of("booleanField", LegacySQLTypeName.legacySQLTypeName(StandardSQLTypeName.BOOL)),
//                                    Field.of("dateField", LegacySQLTypeName.legacySQLTypeName(StandardSQLTypeName.DATE)));
//
//                    StandardTableDefinition tableDefinition =
//                            StandardTableDefinition.newBuilder()
//                                    .setSchema(schema)
//                                    .setTimePartitioning(partitioning)
//                                    .build();
                } catch (Exception var5) {
                    context.output(AdDatasetTransform.this.failureTag(), FailsafeElement.of(element).setErrorMessage(var5.getMessage()).setStacktrace(Throwables.getStackTraceAsString(var5)));
                }
            }
        }).withOutputTags(this.successTag(), TupleTagList.of(this.failureTag())));
    }

    static String getAdDatasetCreateQuery(RawEvent rawEvent) {
        return "CREATE OR REPLACE TABLE akka_events." + rawEvent.getPublisherCode() + "_Ad_generated (" +
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
    static String getAdDatasetSelectQuery(RawEvent rawEvent) {
        return "select *  from akka_events." + rawEvent.getPublisherCode() + "_Ad_generated " +
                "FOR JSON AUTO"; }

    @com.google.auto.value.AutoValue.Builder
    public static class Builder<T> {
        private TupleTag<FailsafeElement<T, Object>> successTag;
        private TupleTag<FailsafeElement<T, Object>> failureTag;
        public Builder() {
        }
        public AdDatasetTransform.Builder<T> setSuccessTag(TupleTag<FailsafeElement<T, Object>> successTag){
            if (successTag == null) {
                throw new NullPointerException("Null successTag");
            } else {
                this.successTag = successTag;
                return this;
            }
        }

        public  AdDatasetTransform.Builder<T> setFailureTag(TupleTag<FailsafeElement<T, Object>> failureTag){
            if (failureTag == null) {
                throw new NullPointerException("Null failureTag");
            } else {
                this.failureTag = failureTag;
                return this;
            }
        }

        public  AdDatasetTransform<T> build(){
            String missing = "";
            if (this.successTag == null) {
                missing = missing + " successTag";
            }

            if (this.failureTag == null) {
                missing = missing + " failureTag";
            }

            if (!missing.isEmpty()) {
                throw new IllegalStateException("Missing required properties:" + missing);
            } else {
                return new AdDatasetTransform(this.successTag, this.failureTag);
            }
        }

    }
}
