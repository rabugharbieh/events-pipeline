package com.atypon.transforms;

import com.atypon.data.adDataset.RawEvent;
import com.atypon.queries.dataset.AbstractDatasetQuery;
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
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

import java.util.UUID;


import java.nio.charset.StandardCharsets;

public class CreateDatasetEventsTableTransform extends PTransform<PCollection<PubsubMessage>, PCollection<FailsafeElement<PubsubMessage, String>>> {
    private String datasetName;
    private TupleTag<PubsubMessage> successTag;
    private TupleTag<FailsafeElement> failureTag;
    private BigQuery bigquery;

    public CreateDatasetEventsTableTransform(String datasetName,
                                             TupleTag<PubsubMessage> successTag,
                                             TupleTag<FailsafeElement> failureTag) {
        this.datasetName = datasetName;
        this.successTag = successTag;
        this.failureTag = failureTag;
        this.bigquery = BigQueryOptions.getDefaultInstance().getService();

    }

    public static CreateDatasetEventsTableTransform.Builder newBuilder() {
        return new CreateDatasetEventsTableTransform.Builder();
    }

    public TupleTag<PubsubMessage> successTag() {
        return this.successTag;
    }

    public TupleTag<FailsafeElement> failureTag() {
        return this.failureTag;
    }

    @Override
    public PCollection<FailsafeElement<PubsubMessage, String>> expand(PCollection<PubsubMessage> input) {
        return input
                // Map the incoming messages into FailsafeElements so we can recover from failures
                // across multiple transforms.
                .apply("MapToRecord", ParDo.of(new CreateDatasetEventsTableTransform.PubsubMessageToFailsafeElementFn())).
                        apply("ProcessTransform", ParDo.of(new DoFn<FailsafeElement<PubsubMessage, String>, FailsafeElement<PubsubMessage, String>>() {

                                    @ProcessElement
                                    public void processElement(ProcessContext context) {
                                        FailsafeElement<PubsubMessage, String> element = context.element();
                                        String payloadStr = element.getPayload();
                                        ObjectMapper mapper = new ObjectMapper();
                                        try {
                                            RawEvent rawEvent = mapper.readValue(payloadStr, RawEvent.class);
                                            AbstractDatasetQuery abstractDatasetQuery = AbstractDatasetQuery.getDatasetQueryInstance(datasetName, rawEvent);
                                            if (rawEvent != null) {
                                                String query = abstractDatasetQuery.getQuery();
                                                QueryJobConfiguration queryConfig =
                                                        QueryJobConfiguration.newBuilder(query).setUseLegacySql(false)
                                                                .build();
                                                // Create a job ID so that we can safely retry.
                                                JobId jobId = JobId.of(UUID.randomUUID().toString());
                                                Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
                                                queryJob = queryJob.waitFor();

                                                // Check for errors
                                                if (queryJob == null) {
                                                    throw new RuntimeException("Job no longer exists");
                                                } else if (queryJob.getStatus().getError() != null) {
                                                    // You can also look at queryJob.getStatus().getExecutionErrors() for all
                                                    // errors, not just the latest one.
                                                    throw new RuntimeException(queryJob.getStatus().getError().toString());
                                                }
                                                context.output(successTag(), element.getOriginalPayload());
                                            }else
                                                throw new RuntimeException("");
                                        } catch (Exception var5) {
                                            context.output(failureTag(), FailsafeElement.of(element).setErrorMessage(var5.getMessage()).setStacktrace(Throwables.getStackTraceAsString(var5)));
                                        }
                                    }
                                })
                        );
    }

    /**
     * The {@link CreateDatasetEventsTableTransform.PubsubMessageToFailsafeElementFn} wraps an incoming {@link PubsubMessage} with the
     * {@link FailsafeElement} class so errors can be recovered from and the original message can be
     * output to a error records table.
     */
    static class PubsubMessageToFailsafeElementFn
            extends DoFn<PubsubMessage, FailsafeElement<PubsubMessage, String>> {
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
        private TupleTag<PubsubMessage> successTag;
        private TupleTag<FailsafeElement> failureTag;

        public Builder() {
        }

        public CreateDatasetEventsTableTransform.Builder setSuccessTag(TupleTag<PubsubMessage> successTag) {
            if (successTag == null) {
                throw new NullPointerException("Null successTag");
            } else {
                this.successTag = successTag;
                return this;
            }
        }

        public CreateDatasetEventsTableTransform.Builder setFailureTag(TupleTag<FailsafeElement> failureTag) {
            if (failureTag == null) {
                throw new NullPointerException("Null failureTag");
            } else {
                this.failureTag = failureTag;
                return this;
            }
        }

        public CreateDatasetEventsTableTransform.Builder setDatasetName(String datasetName) {
            if (datasetName == null) {
                throw new NullPointerException("Null datasetName");
            } else {
                this.datasetName = datasetName;
                return this;
            }
        }

        public CreateDatasetEventsTableTransform build() {
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
                return new CreateDatasetEventsTableTransform(this.datasetName, this.successTag, this.failureTag);
            }
        }

    }
}
