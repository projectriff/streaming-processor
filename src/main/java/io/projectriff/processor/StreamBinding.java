package io.projectriff.processor;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * A StreamBinding captures the name of a topic as well as the address of the (liiklus) gateway
 * that is responsible for it and possible metadata.
 * This allows riff to support multiple gateways (and hence maybe multiple
 * backing broker technologies) until liiklus supports that itself, if ever.
 *
 * @author Florent Biville
 * @author Eric Bottard
 */
public class StreamBinding {

    public static final String CONTENT_TYPE = "contentType";

    private final String gatewayAddress;

    private final String topic;

    private final Map<String, String> metadata;

    public StreamBinding(String gatewayAddress, String topic) {
        this(gatewayAddress, topic, Collections.emptyMap());
    }

    public StreamBinding(String gatewayAddress, String topic, Map<String, String> metadata) {
        this.gatewayAddress = gatewayAddress;
        this.topic = topic;
        this.metadata = metadata;
    }

    public String getGatewayAddress() {
        return gatewayAddress;
    }

    public String getTopic() {
        return topic;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamBinding that = (StreamBinding) o;
        return Objects.equals(gatewayAddress, that.gatewayAddress) &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(gatewayAddress, topic, metadata);
    }

    @Override
    public String toString() {
        return "StreamBinding{" +
                "gatewayAddress='" + gatewayAddress + '\'' +
                ", topic='" + topic + '\'' +
                ", metadata='" + metadata + '\'' +
                '}';
    }
}
