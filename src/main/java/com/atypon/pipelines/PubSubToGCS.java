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

import com.google.cloud.teleport.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.util.DurationUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * This pipeline ingests incoming data from a Cloud Pub/Sub topic and
 * outputs the raw data into windowed files at the specified output
 * directory at GCS.
 */
public class PubSubToGCS {

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.</p>
   */
  public interface PubSubToGCSOptions extends PipelineOptions, StreamingOptions {
    @Description("The Cloud Pub/Sub topic to read from.")
    String getInputTopic();
    void setInputTopic(String value);

    @Description("The directory to output files to. Must end with a slash.")
    String getOutputDirectory();
    void setOutputDirectory(String value);

    @Description("The filename prefix of the files to write to.")
    @Default.String("output")
    String getOutputFilenamePrefix();
    void setOutputFilenamePrefix(String value);

    @Description("The suffix of the files to write.")
    @Default.String("")
    String getOutputFilenameSuffix();
    void setOutputFilenameSuffix(String value);

    @Description("The shard template of the output file. Specified as repeating sequences "
        + "of the letters 'S' or 'N' (example: SSS-NNN). These are replaced with the "
        + "shard number, or number of shards respectively")
    @Default.String("W-P-SS-of-NN")
    String getOutputShardTemplate();
    void setOutputShardTemplate(String value);

    @Description("The maximum number of output shards produced when writing.")
    @Default.Integer(1)
    Integer getNumShards();
    void setNumShards(Integer value);

    @Description("The window duration in which data will be written. Defaults to 5m. "
        + "Allowed formats are: "
        + "Ns (for seconds, example: 5s), "
        + "Nm (for minutes, example: 12m), "
        + "Nh (for hours, example: 2h).")
    @Default.String("5m")
    String getWindowDuration();
    void setWindowDuration(String value);
  }

  private static Properties getConfig() {
    try {
      File file = new File("src/main/conf/config.xml");
      FileInputStream fileInput = new FileInputStream(file);
      Properties properties = new Properties();
      properties.loadFromXML(fileInput);
      fileInput.close();
      return properties;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Main entry point for executing the pipeline.
   * @param args  The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    Properties config = getConfig();
    PubSubToGCSOptions options = PipelineOptionsFactory
        .fromArgs(args)
        .withValidation()
        .as(PubSubToGCSOptions.class);

    options.setInputTopic(config.getProperty("pubSupToGCSTopic"));
    options.setWindowDuration(config.getProperty("windowDuration"));
    options.setNumShards(Integer.parseInt(config.getProperty("numShards")));
    options.setOutputDirectory(config.getProperty("outputDirectory"));
    options.setOutputFilenamePrefix(config.getProperty("outputFileNamePrefix"));
    options.setOutputFilenameSuffix(config.getProperty("outputFileNameSuffix"));
    options.setOutputShardTemplate(config.getProperty("outputShardTemplate"));
    options.setStreaming(true);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return  The result of the pipeline execution.
   */
  public static PipelineResult run(PubSubToGCSOptions options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    /*
     * Steps:
     *   1) Read string messages from PubSub
     *   2) Window the messages into minute intervals specified by the executor.
     *   3) Output the windowed files to GCS
     */
    pipeline
        .apply("Read PubSub Events", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
        .apply(
            options.getWindowDuration() + " Window",
            Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))

        // Apply windowed file writes. Use a NestedValueProvider because the filename
        // policy requires a resourceId generated from the input value at runtime.
        .apply(
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
                .withTempDirectory(NestedValueProvider.of(
                        (ValueProvider) ValueProvider.StaticValueProvider.of(options.getOutputDirectory()),
                    (SerializableFunction<String, ResourceId>) input ->
                        FileBasedSink.convertToFileResourceIfPossible(input))));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}
