package io.github.persiliao.mqtt;

/**
 * Thrown when an MQTT payload cannot be converted to the parameter type
 * declared by a handler method (for example a malformed JSON payload for a
 * {@code Map} or POJO parameter).
 *
 * <p>The failure is recorded in the handler statistics; if the bean declares
 * additional handler methods the next one is tried, otherwise the message is
 * counted as a failure and logged.
 *
 * @since 3.0.0
 */
public class MqttMessageConversionException extends RuntimeException {

    /**
     * Creates a new conversion exception.
     *
     * @param message the failure description
     * @param cause   the underlying conversion error (may be {@code null})
     */
    public MqttMessageConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}
