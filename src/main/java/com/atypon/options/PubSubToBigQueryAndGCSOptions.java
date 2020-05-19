package com.atypon.options;

import com.google.cloud.teleport.templates.common.JavascriptTextTransformer;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;

public interface PubSubToBigQueryAndGCSOptions extends JavascriptTextTransformer.JavascriptTextTransformerOptions  {
    @Description("Pub/Sub topic to read the input from")
    ValueProvider<String> getInputTopic();
    void setInputTopic(ValueProvider<String> value);


    @Description(
            "The Cloud Pub/Sub subscription to consume from. "
                    + "The name should be in the format of "
                    + "projects/<project-id>/subscriptions/<subscription-name>.")
    ValueProvider<String> getInputSubscription();
    void setInputSubscription(ValueProvider<String> value);

    @Description(
            "This determines whether the template reads from " + "a pub/sub subscription or a topic")
    @Default.Boolean(false)
    Boolean getUseSubscription();
    void setUseSubscription(Boolean value);


    @Description(
            "The dead-letter table to output to within BigQuery in <project-id>:<dataset>.<table> "
                    + "format. If it doesn't exist, it will be created during pipeline execution.")
    ValueProvider<String> getOutputDeadletterTable();
    void setOutputDeadletterTable(ValueProvider<String> value);

    @Description("This determines the dataset query to use ")
    @Default.Boolean(false)
    ValueProvider<String> getDatasetName();
    void setDatasetName(ValueProvider<String> value);


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

    @Description("The maximum number of output shards produced when writing.")
    @Default.Integer(1)
    Integer getNumShards();
    void setNumShards(Integer value);


    @Description("The shard template of the output file. Specified as repeating sequences "
            + "of the letters 'S' or 'N' (example: SSS-NNN). These are replaced with the "
            + "shard number, or number of shards respectively")
    @Default.String("W-P-SS-of-NN")
    String getOutputShardTemplate();
    void setOutputShardTemplate(String value);

    @Description("The window duration in which data will be written. Defaults to 5m. "
            + "Allowed formats are: "
            + "Ns (for seconds, example: 5s), "
            + "Nm (for minutes, example: 12m), "
            + "Nh (for hours, example: 2h).")
    @Default.String("5m")
    String getWindowDuration();
    void setWindowDuration(String value);

}
