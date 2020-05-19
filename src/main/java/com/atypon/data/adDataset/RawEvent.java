
package com.atypon.data.adDataset;

import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.apache.commons.net.ntp.TimeStamp;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
public class RawEvent
{
    @JsonProperty("eventID")
    private String eventID;
    @JsonProperty("publisherCode")
    private String publisherCode;
    @JsonProperty("sessionID")
    private Integer sessionID;
    @JsonProperty("platform")
    private String platform;
    @JsonProperty("identityID")
    private Integer identityID;
    @JsonProperty("contentID")
    private Integer contentID;
    @JsonProperty("subsessionID")
    private Integer subsessionID;
    @JsonProperty("timestamp")
    private TimeStamp timestamp;
    @JsonProperty("siteCode")
    private String siteCode;
    @JsonProperty("ipAddress")
    private String ipAddress;
    @JsonProperty("eventType")
    private String eventType;
    @JsonProperty("origination")
    private String origination;
    @JsonProperty("productID")
    private Integer productID;
    @JsonProperty("placeholderID")
    private Integer placeholderID;
    @JsonProperty("adID")
    private Integer adID;
    @JsonProperty("uniqueImpression")
    private Boolean uniqueImpression;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    /**
     * No args constructor for use in serialization
     * 
     */
    public RawEvent() {
    }

    /**
     * 
     * @param eventID
     * @param siteCode
     * @param productID
     * @param publisherCode
     * @param contentID
     * @param ipAddress
     * @param sessionID
     * @param eventType
     * @param platform
     * @param placeholderID
     * @param adID
     * @param identityID
     * @param origination
     * @param uniqueImpression
     * @param subsessionID
     * @param timestamp
     */
    public RawEvent(String eventID, String publisherCode, Integer sessionID, String platform, Integer identityID, Integer contentID, Integer subsessionID, TimeStamp timestamp, String siteCode, String ipAddress, String eventType, String origination, Integer productID, Integer placeholderID, Integer adID, Boolean uniqueImpression) {
        super();
        this.eventID = eventID;
        this.publisherCode = publisherCode;
        this.sessionID = sessionID;
        this.platform = platform;
        this.identityID = identityID;
        this.contentID = contentID;
        this.subsessionID = subsessionID;
        this.timestamp = timestamp;
        this.siteCode = siteCode;
        this.ipAddress = ipAddress;
        this.eventType = eventType;
        this.origination = origination;
        this.productID = productID;
        this.placeholderID = placeholderID;
        this.adID = adID;
        this.uniqueImpression = uniqueImpression;
    }

}
