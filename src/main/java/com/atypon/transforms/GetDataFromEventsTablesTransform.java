package com.atypon.transforms;

import com.atypon.data.adDataset.RawEvent;
import com.atypon.queries.SelectAllQuery;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.base.Throwables;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.codehaus.jackson.map.ObjectMapper;


import java.nio.charset.StandardCharsets;

public class GetDataFromEventsTablesTransform extends PTransform<PCollection<PubsubMessage>,
        PCollection<FailsafeElement<PubsubMessage, Object>>> {
    private String datasetName;
    private TupleTag<BigQueryIO.TypedRead<TableRow>> successTag;
    private TupleTag<FailsafeElement> failureTag;

    public GetDataFromEventsTablesTransform(String datasetName,
                                            TupleTag<BigQueryIO.TypedRead<TableRow>> successTag,
                                            TupleTag<FailsafeElement> failureTag) {
        this.datasetName = datasetName;
        this.successTag = successTag;
        this.failureTag = failureTag;
    }

    public TupleTag<BigQueryIO.TypedRead<TableRow>> successTag() {
        return this.successTag;
    }

    public TupleTag<FailsafeElement> failureTag() {
        return this.failureTag;
    }

    public static GetDataFromEventsTablesTransform.Builder newBuilder() {
        return new GetDataFromEventsTablesTransform.Builder();
    }

    @Override
    public PCollection<FailsafeElement<PubsubMessage, Object>> expand(PCollection<PubsubMessage> input) {
        return input
                // Map the incoming messages into FailsafeElements so we can recover from failures
                // across multiple transforms.
                .apply("MapToRecord", ParDo.of(new GetDataFromEventsTablesTransform.PubsubMessageToFailsafeElementFn())).
                        apply("ProcessTransform", ParDo.of(new DoFn<FailsafeElement<PubsubMessage, Object>, FailsafeElement<PubsubMessage, Object>>() {

                                    @ProcessElement
                                    public void processElement(ProcessContext context) {
                                        FailsafeElement<PubsubMessage, Object> element = context.element();
                                        String payloadStr = element.getPayload().toString();
                                        ObjectMapper mapper = new ObjectMapper();
                                        try {
                                            RawEvent rawEvent = mapper.readValue(payloadStr, RawEvent.class);
                                            BigQueryIO.TypedRead<TableRow> result;
                                            String tableName = rawEvent.getPublisherCode() + "_Ad_generated";

                                            SelectAllQuery queryInstance = new SelectAllQuery(tableName, datasetName);
                                            if (rawEvent != null) {
                                                String query = queryInstance.getQuery();
                                                result = BigQueryIO.readTableRows()
                                                        .fromQuery(query);
                                                context.output(successTag(), result);
                                            }
                                        } catch (Exception var5) {
                                            context.output(failureTag(), FailsafeElement.of(element).setErrorMessage(var5.getMessage()).setStacktrace(Throwables.getStackTraceAsString(var5)));
                                        }
                                    }
                                })
                        );
    }

    /**
     * The {@link GetDataFromEventsTablesTransform.PubsubMessageToFailsafeElementFn} wraps an incoming {@link PubsubMessage} with the
     * {@link FailsafeElement} class so errors can be recovered from and the original message can be
     * output to a error records table.
     */
    static class PubsubMessageToFailsafeElementFn
            extends DoFn<PubsubMessage, FailsafeElement<PubsubMessage, Object>> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            PubsubMessage message = context.element();
            context.output(
                    FailsafeElement.of(message, new String(message.getPayload(), StandardCharsets.UTF_8)));
        }
    }

    @com.google.auto.value.AutoValue.Builder
    public static class Builder {
        private String datasetName;
        private TupleTag<BigQueryIO.TypedRead<TableRow>> successTag;
        private TupleTag<FailsafeElement> failureTag;

        public Builder() {
        }

        public GetDataFromEventsTablesTransform.Builder setSuccessTag(TupleTag<BigQueryIO.TypedRead<TableRow>> successTag) {
            if (successTag == null) {
                throw new NullPointerException("Null successTag");
            } else {
                this.successTag = successTag;
                return this;
            }
        }

        public GetDataFromEventsTablesTransform.Builder setFailureTag(TupleTag<FailsafeElement> failureTag) {
            if (failureTag == null) {
                throw new NullPointerException("Null failureTag");
            } else {
                this.failureTag = failureTag;
                return this;
            }
        }

        public GetDataFromEventsTablesTransform.Builder setDatasetName(String datasetName) {
            if (datasetName == null) {
                throw new NullPointerException("Null datasetName");
            } else {
                this.datasetName = datasetName;
                return this;
            }
        }

        public GetDataFromEventsTablesTransform build() {
            String missing = "";
            if (this.successTag == null) {
                missing = missing + " successTag";
            }

            if (this.failureTag == null) {
                missing = missing + " failureTag";
            }
            if (this.datasetName == null) {
                missing = missing + " datasetName";
            }

            if (!missing.isEmpty()) {
                throw new IllegalStateException("Missing required properties:" + missing);
            } else {
                return new GetDataFromEventsTablesTransform(this.datasetName, this.successTag, this.failureTag);
            }
        }

    }
}
