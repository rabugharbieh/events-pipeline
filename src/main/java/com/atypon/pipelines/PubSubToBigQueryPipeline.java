/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.atypon.pipelines;

import com.atypon.common.ConfigurationReader;
import com.atypon.options.PubSubToBigQueryAndGCSOptions;
import com.atypon.transforms.CreateDatasetEventsTableTransform;
import com.atypon.transforms.GetDataFromEventsTablesTransform;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.util.DurationUtils;
import com.google.cloud.teleport.values.FailsafeElement;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.configuration.Configuration;

/**
 * The {@link PubSubToBigQueryPipeline} pipeline is a streaming pipeline which ingests data in JSON format
 * from Cloud Pub/Sub, executes a UDF, and outputs the resulting records to BigQuery. Any errors
 * which occur in the transformation of the data or execution of the UDF will be output to a
 * separate errors table in BigQuery. The errors table will be created if it does not exist prior to
 * execution. Both output and error tables are specified by the user as template parameters.
 */
public class PubSubToBigQueryPipeline {

    /**
     * The log to output status messages to.
     */
    private static final Logger LOG = LoggerFactory.getLogger(PubSubToBigQueryPipeline.class);

    /**
     * success and failure outputs of create dataset table
     */
    private static final TupleTag<PubsubMessage> CREATE_SUCCESS_TAG = new TupleTag<PubsubMessage>() {
    };
    private static final TupleTag<FailsafeElement> CREATE_FAILURE_TAG = new TupleTag<FailsafeElement>() {
    };


    /**
     * The tag for the main output of the json transformation.
     */
    public static final TupleTag<BigQueryIO.TypedRead<TableRow>> GET_EVENT_DATA_SUCCESS_TAG = new TupleTag<BigQueryIO.TypedRead<TableRow>>() {
    };
    public static final TupleTag<FailsafeElement> GET_EVENT_DATA_FAILURE_TAG = new TupleTag<FailsafeElement>() {
    };


    /**
     * Pubsub message/string coder for pipeline.
     */
    public static final FailsafeElementCoder<PubsubMessage, String> CODER =
            FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

    /**
     * String/String Coder for FailsafeElement.
     */
    public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
            FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    /**
     * The main entry-point for pipeline execution. This method will start the pipeline but will not
     * wait for it's execution to finish. If blocking execution is required, use the {@link
     * PubSubToBigQueryPipeline#run(PubSubToBigQueryAndGCSOptions, Configuration)} } method to start the pipeline and invoke {@code
     * result.waitUntilFinish()} on the {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) throws Exception {
        ConfigurationReader configurationReader = new ConfigurationReader();
        Configuration config = configurationReader.getConfiguration();
        PubSubToBigQueryAndGCSOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToBigQueryAndGCSOptions.class);
        run(options, config);
    }

    /**
     * Runs the pipeline to completion with the specified options. This method does not wait until the
     * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
     * object to block until the pipeline is finished running if blocking programmatic execution is
     * required.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    public static PipelineResult run(PubSubToBigQueryAndGCSOptions options, Configuration config) {

        Pipeline pipeline = Pipeline.create(options);

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(CODER.getEncodedTypeDescriptor(), CODER);

        PCollection<PubsubMessage> messages = null;
        messages =
                pipeline.apply(
                        "ReadPubSubTopic",
                        PubsubIO.readMessagesWithAttributes().fromTopic(config.getString("pubSupToBQTopic")));

        PCollection<FailsafeElement<PubsubMessage, String>> createDatasetEventsTableTransform =
                messages.apply("CreateDatasetEventsTableTransform", CreateDatasetEventsTableTransform.newBuilder()
                        .setDatasetName(options.getDatasetName().get())
                        .setSuccessTag(CREATE_SUCCESS_TAG)
                        .setFailureTag(CREATE_FAILURE_TAG).build());
        writeToGCS(options , messages);

        return pipeline.run();
    }

    public static void writeToGCS(PubSubToBigQueryAndGCSOptions options, PCollection<PubsubMessage> messages) {
        PCollection<FailsafeElement<PubsubMessage, Object>> getDataFromEventsTablesTransform =
                messages.apply("GetDataFromEventsTablesTransform", GetDataFromEventsTablesTransform.newBuilder()
                        .setDatasetName(options.getDatasetName().get())
                        .setSuccessTag(GET_EVENT_DATA_SUCCESS_TAG)
                        .setFailureTag(GET_EVENT_DATA_FAILURE_TAG).build());


        getDataFromEventsTablesTransform.apply("convert to jason", ParDo.of(new DoFn<FailsafeElement<PubsubMessage, Object>, String>() {
            @DoFn.ProcessElement
            public void processElement(DoFn.ProcessContext context) {
                Object element = context.element();
                String payloadStr = element.toString();
                context.output(payloadStr);
            }
        }))
                .apply(options.getWindowDuration() + " Window",
                        Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration())))).apply(
                "Write File(s)",
                TextIO.write()
                        .withWindowedWrites()
                        .withNumShards(options.getNumShards())
                        .to(
                                new WindowedFilenamePolicy(
                                        options.getOutputDirectory(),
                                        options.getOutputFilenamePrefix(),
                                        options.getOutputShardTemplate(),
                                        options.getOutputFilenameSuffix()))
                        .withTempDirectory(ValueProvider.NestedValueProvider.of(
                                (ValueProvider) ValueProvider.StaticValueProvider.of(options.getOutputDirectory()),
                                (SerializableFunction<String, ResourceId>) input ->
                                        FileBasedSink.convertToFileResourceIfPossible(input))));
    }


    static class ValueProviderImpl implements ValueProvider<String> {

        private String value;

        public String get() {
            return value;
        }

        public boolean isAccessible() {
            return true;
        }

        public void set(String value) {
            this.value = value;
        }
    }
}
