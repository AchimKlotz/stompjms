/**
 * Copyright (C) 2010-2011, FuseSource Corp.  All rights reserved.
 *
 *     http://fusesource.com
 *
 * The software in this package is published under the terms of the
 * CDDL license a copy of which has been included with this distribution
 * in the license.txt file.
 */

package org.fusesource.stomp.jms.message;

import static org.fusesource.hawtbuf.Buffer.ascii;
import static org.fusesource.stomp.client.Constants.CONTENT_LENGTH;
import static org.fusesource.stomp.client.Constants.CORRELATION_ID;
import static org.fusesource.stomp.client.Constants.DESTINATION;
import static org.fusesource.stomp.client.Constants.EXPIRATION_TIME;
import static org.fusesource.stomp.client.Constants.FALSE;
import static org.fusesource.stomp.client.Constants.JMSX_DELIVERY_COUNT;
import static org.fusesource.stomp.client.Constants.MESSAGE;
import static org.fusesource.stomp.client.Constants.MESSAGE_ID;
import static org.fusesource.stomp.client.Constants.PERSISTENT;
import static org.fusesource.stomp.client.Constants.PRIORITY;
import static org.fusesource.stomp.client.Constants.RECEIPT_REQUESTED;
import static org.fusesource.stomp.client.Constants.REDELIVERED;
import static org.fusesource.stomp.client.Constants.REPLY_TO;
import static org.fusesource.stomp.client.Constants.SUBSCRIPTION;
import static org.fusesource.stomp.client.Constants.TIMESTAMP;
import static org.fusesource.stomp.client.Constants.TRANSFORMATION;
import static org.fusesource.stomp.client.Constants.TRUE;
import static org.fusesource.stomp.client.Constants.TYPE;
import static org.fusesource.stomp.codec.StompFrame.NO_DATA;
import static org.fusesource.stomp.codec.StompFrame.decodeHeader;
import static org.fusesource.stomp.codec.StompFrame.encodeHeader;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Callable;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;

import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.stomp.codec.StompFrame;
import org.fusesource.stomp.jms.StompJmsConnection;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.StompJmsExceptionSupport;
import org.fusesource.stomp.jms.util.PropertyExpression;
import org.fusesource.stomp.jms.util.TypeConversionSupport;

public class StompJmsMessage implements javax.jms.Message {

    private static final Map<String, PropertySetter> JMS_PROPERTY_SETERS = new HashMap<String, PropertySetter>();

    static HashSet<AsciiBuffer> RESERVED_HEADER_NAMES = new HashSet<AsciiBuffer>();
    static{
        RESERVED_HEADER_NAMES.add(DESTINATION);
        RESERVED_HEADER_NAMES.add(REPLY_TO);
        RESERVED_HEADER_NAMES.add(MESSAGE_ID);
        RESERVED_HEADER_NAMES.add(CORRELATION_ID);
        RESERVED_HEADER_NAMES.add(EXPIRATION_TIME);
        RESERVED_HEADER_NAMES.add(TIMESTAMP);
        RESERVED_HEADER_NAMES.add(PRIORITY);
        RESERVED_HEADER_NAMES.add(REDELIVERED);
        RESERVED_HEADER_NAMES.add(TYPE);
        RESERVED_HEADER_NAMES.add(PERSISTENT);
        RESERVED_HEADER_NAMES.add(RECEIPT_REQUESTED);
        RESERVED_HEADER_NAMES.add(TRANSFORMATION);
        RESERVED_HEADER_NAMES.add(SUBSCRIPTION);
        RESERVED_HEADER_NAMES.add(CONTENT_LENGTH);
        RESERVED_HEADER_NAMES.add(JMSX_DELIVERY_COUNT);
    }

    public static HashSet<AsciiBuffer> REVERSED_HEADER_NAMES = new HashSet<AsciiBuffer>();
    static{
        REVERSED_HEADER_NAMES.add(DESTINATION);
        REVERSED_HEADER_NAMES.add(REPLY_TO);
    }

    public static enum JmsMsgType {
        MESSAGE("jms/message"),
        BYTES("jms/bytes-message"),
        MAP("jms/map-message"),
        OBJECT("jms/object-message"),
        STREAM("jms/stream-message"),
        TEXT("jms/text-message"),
        TEXT_NULL("jms/text-message-null");

        public final AsciiBuffer buffer = new AsciiBuffer(this.name());
        public final AsciiBuffer mime;

        JmsMsgType(String mime){
            this.mime = (ascii(mime));
        }
    }

    protected transient Callable<Void> acknowledgeCallback;
    protected transient StompJmsConnection connection;

    protected boolean readOnlyBody;
    protected boolean readOnlyProperties;
    protected Map<String, Object> properties;
    protected AsciiBuffer transactionId;
    protected StompFrame frame = new StompFrame(MESSAGE);

    public StompJmsMessage() {
        getHeaderMap().put(TRANSFORMATION, getMsgType().buffer);
    }

    /**
     * @throws JMSException in child implementations
     */
    public StompJmsMessage copy() throws JMSException {
        StompJmsMessage other = new StompJmsMessage();
        other.copy(this);
        return other;
    }

    public JmsMsgType getMsgType() {
        return JmsMsgType.MESSAGE;
    }

    public StompFrame getFrame() {
        return frame;
    }

    public void setFrame(StompFrame frame) {
        this.frame = frame;
    }

    protected void copy(StompJmsMessage other) {
        this.readOnlyBody = other.readOnlyBody;
        this.readOnlyProperties = other.readOnlyBody;
        if (other.properties != null) {
            this.properties = new HashMap<String, Object>(other.properties);
        } else {
            this.properties = null;
        }
        this.frame = other.frame.clone();
        this.acknowledgeCallback = other.acknowledgeCallback;
        this.transactionId = other.transactionId;
        this.connection = other.connection;
    }

    @Override
    public int hashCode() {
        String id = getJMSMessageID();
        if (id != null) {
            return id.hashCode();
        }
        return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != getClass()) {
            return false;
        }

        StompJmsMessage msg = (StompJmsMessage) o;
        String oMsg = msg.getJMSMessageID();
        String thisMsg = this.getJMSMessageID();
        return thisMsg != null && oMsg != null && oMsg.equals(thisMsg);
    }

    @Override
    public void acknowledge() throws JMSException {
        if (acknowledgeCallback != null) {
            try {
                acknowledgeCallback.call();
            } catch (Throwable e) {
                throw StompJmsExceptionSupport.create(e);
            }
        }
    }

    public Buffer getContent() {
        Buffer content = this.frame.content();
        if( content.isEmpty() ) {
            return null;
        }
        return content;
    }

    public void setContent(Buffer content) {
        if( content == null ) {
            this.frame.content(NO_DATA);
        } else {
            this.frame.content(content);
        }
    }

    @Override
    public void clearBody() throws JMSException {
        if (this.frame != null) {
            this.frame.content(StompFrame.NO_DATA);
        }
        setContent(null);
        readOnlyBody = false;
    }

    public void setReadOnlyBody(boolean readOnlyBody) {
        this.readOnlyBody = readOnlyBody;
    }

    public void setReadOnlyProperties(boolean readOnlyProperties) {
        this.readOnlyProperties = readOnlyProperties;
    }

    private String getStringHeader(AsciiBuffer key) {
        AsciiBuffer buffer = getHeaderMap().get(key);
        if( buffer == null ) {
            return null;
        }
        return buffer.toString();
    }
    private void setStringHeader(AsciiBuffer key, String value) {
        if(value==null) {
            getHeaderMap().remove(key);
        } else {
            getHeaderMap().put(key, ascii(value));
        }
    }

    private byte[] getBytesHeader(AsciiBuffer key) {
        AsciiBuffer buffer = getHeaderMap().get(key);
        if( buffer == null ) {
            return null;
        }
        return buffer.deepCopy().data;
    }
    private void setBytesHeader(AsciiBuffer key, byte[]  value) {
        if(value==null) {
            getHeaderMap().remove(key);
        } else {
            getHeaderMap().put(key, new Buffer(value).deepCopy().ascii());
        }
    }

    private Integer getIntegerHeader(AsciiBuffer key) {
        AsciiBuffer buffer = getHeaderMap().get(key);
        if( buffer == null ) {
            return null;
        }
        return Integer.parseInt(buffer.toString());
    }
    private void setIntegerHeader(AsciiBuffer key, Integer value) {
        if(value==null) {
            getHeaderMap().remove(key);
        } else {
            getHeaderMap().put(key, ascii(value.toString()));
        }
    }

    private Long getLongHeader(AsciiBuffer key) {
        AsciiBuffer buffer = getHeaderMap().get(key);
        if( buffer == null ) {
            return null;
        }
        return Long.parseLong(buffer.toString());
    }
    private void setLongHeader(AsciiBuffer key, Long value) {
        if(value==null) {
            getHeaderMap().remove(key);
        } else {
            getHeaderMap().put(key, ascii(value.toString()));
        }
    }

    private Boolean getBooleanHeader(AsciiBuffer key) {
        AsciiBuffer buffer = getHeaderMap().get(key);
        if( buffer == null ) {
            return null;
        }
        return Boolean.parseBoolean(buffer.toString());
    }
    private void setBooleanHeader(AsciiBuffer key, Boolean value) {
        if(value==null) {
            getHeaderMap().remove(key);
        } else {
            getHeaderMap().put(key, value.booleanValue() ? TRUE : FALSE);
        }
    }

    private StompJmsDestination getDestinationHeader(AsciiBuffer key) throws JMSException {
        AsciiBuffer buffer = getHeaderMap().get(key);
        if( buffer == null ) {
            return null;
        }
        return StompJmsDestination.createDestination(connection, buffer.toString());
    }
    private void setDestinationHeader(AsciiBuffer key, StompJmsDestination value) {
        if(value==null) {
            getHeaderMap().remove(key);
        } else {
            getHeaderMap().put(key, ascii(value.toString()));
        }
    }

    public AsciiBuffer getMessageID() {
        return getHeaderMap().get(MESSAGE_ID);
    }

    @Override
    public String getJMSMessageID() {
        return getStringHeader(MESSAGE_ID);
    }

    /**
     * Seems to be invalid because the parameter doesn't initialize MessageId
     * instance variables ProducerId and ProducerSequenceId
     *
     * @param value
     * @throws JMSException
     */
    @Override
    public void setJMSMessageID(String value) {
        setStringHeader(MESSAGE_ID, value);
    }
    public void setMessageID(AsciiBuffer value) {
        getHeaderMap().put(MESSAGE_ID, value);
    }


    private <T> T or(T value, T other) {
        if(value!=null) {
            return value;
        }
        return other;
    }

    @Override
    public long getJMSTimestamp() {
        return or(getLongHeader(TIMESTAMP), 0L);
    }

    @Override
    public void setJMSTimestamp(long timestamp) {
        setLongHeader(TIMESTAMP, timestamp==0? null : timestamp);
    }

    @Override
    public String getJMSCorrelationID() {
        return getStringHeader(CORRELATION_ID);
    }

    @Override
    public void setJMSCorrelationID(String correlationId) {
        setStringHeader(CORRELATION_ID, correlationId);
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return getBytesHeader(CORRELATION_ID);
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationId) throws JMSException {
        setBytesHeader(CORRELATION_ID, correlationId);
    }


    public boolean isPersistent() {
        return or(getBooleanHeader(PERSISTENT), false);
    }

    public void setPersistent(boolean value) {
        setBooleanHeader(PERSISTENT, value ? true : null);
    }

    protected static String decodeString(byte[] data) throws JMSException {
        try {
            if (data == null) {
                return null;
            }
            return new String(data, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new JMSException("Invalid UTF-8 encoding: " + e.getMessage());
        }
    }

    protected static byte[] encodeString(String data) throws JMSException {
        try {
            if (data == null) {
                return null;
            }
            return data.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new JMSException("Invalid UTF-8 encoding: " + e.getMessage());
        }
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {
        return getStompJmsReplyTo();
    }

    @Override
    public void setJMSReplyTo(Destination destination) throws JMSException {
        setJMSReplyTo(StompJmsMessageTransformation.transformDestination(connection, destination));
    }

    public void setJMSReplyTo(StompJmsDestination destination) {
        setDestinationHeader(REPLY_TO, destination);
    }

    public StompJmsDestination getStompJmsReplyTo() throws JMSException {
        return getDestinationHeader(REPLY_TO);
    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        return getStompJmsDestination();
    }

    public StompJmsDestination getStompJmsDestination() throws JMSException {
        return getDestinationHeader(DESTINATION);
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException {
        setJMSDestination(StompJmsMessageTransformation.transformDestination(connection, destination));
    }

    public void setJMSDestination(StompJmsDestination destination) {
        setDestinationHeader(DESTINATION, destination);
    }

    @Override
    public int getJMSDeliveryMode() {
        return this.isPersistent() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
    }

    @Override
    public void setJMSDeliveryMode(int mode) {
        this.setPersistent(mode == DeliveryMode.PERSISTENT);
    }

    public boolean isRedelivered() {
        return getRedeliveryCounter() > 0;
    }

    public void setRedelivered(boolean redelivered) {
        if (redelivered) {
            if (!isRedelivered()) {
                setRedeliveryCounter(1);
            }
        } else {
            if (isRedelivered()) {
                setRedeliveryCounter(0);
            }
        }
    }

    public int getRedeliveryCounter() {
        AsciiBuffer value = getHeaderMap().get(JMSX_DELIVERY_COUNT);
        if( value == null ) {
            return 0;
        }
        return Integer.parseInt(value.toString());
    }

    public void setRedeliveryCounter(int deliveryCounter) {
        getHeaderMap().put(JMSX_DELIVERY_COUNT, encodeHeader("" + deliveryCounter));
    }

    @Override
    public boolean getJMSRedelivered() {
        return this.isRedelivered();
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) {
        this.setRedelivered(redelivered);
    }

    @Override
    public String getJMSType() {
        return getStringHeader(TYPE);
    }

    @Override
    public void setJMSType(String type) {
        setStringHeader(TYPE, type);
    }

    @Override
    public long getJMSExpiration() {
        return or(getLongHeader(EXPIRATION_TIME), 0L);
    }

    @Override
    public void setJMSExpiration(long expiration) {
        setLongHeader(EXPIRATION_TIME, expiration == 0 ? null : expiration);
    }

    @Override
    public int getJMSPriority() {
        return or(getIntegerHeader(PRIORITY), 4);
    }

    @Override
    public void setJMSPriority(int priority) {
        setIntegerHeader(PRIORITY, priority == 4 ? null : priority);
    }

    public Map<String, Object> getProperties() {
        lazyCreateProperties();
        return Collections.unmodifiableMap(properties);
    }

    @Override
    public void clearProperties() {
        if (this.frame != null) {
            getHeaderMap().clear();
        }
        properties = null;
    }

    public void setProperty(String name, Object value) {
        lazyCreateProperties();
        properties.put(name, value);
        getHeaderMap().put(encodeHeader(name), encodeHeader(value.toString()));
    }

    public void removeProperty(String name) {
        lazyCreateProperties();
        properties.remove(name);
    }

    protected void lazyCreateProperties() {
        if (properties == null) {
            if (this.frame != null) {
                properties = new HashMap<String, Object>(getHeaderMap().size());
                for (Map.Entry<AsciiBuffer, AsciiBuffer> entry: getHeaderMap().entrySet()){
                    if( !RESERVED_HEADER_NAMES.contains(entry.getKey()) ) {
                        properties.put(decodeHeader(entry.getKey()), decodeHeader(entry.getValue()));
                    }
                }
            } else {
                properties = new HashMap<String, Object>();
            }
        }
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        return (this.getProperties().containsKey(name) || getObjectProperty(name) != null);
    }

    @Override
    public Enumeration<String> getPropertyNames() throws JMSException {
        Vector<String> result = new Vector<String>(this.getProperties().keySet());
        return result.elements();
    }

    /**
     * return all property names, including standard JMS properties and JMSX properties
     *
     * @return Enumeration of all property names on this message
     * @throws JMSException
     */
    public Enumeration<String> getAllPropertyNames() throws JMSException {
        Vector<String> result = new Vector<String>(this.getProperties().keySet());
        result.addAll(JMS_PROPERTY_SETERS.keySet());
        return result.elements();
    }

    interface PropertySetter {

        void set(StompJmsConnection connection, StompJmsMessage message, Object value) throws MessageFormatException;
    }

    static {
        JMS_PROPERTY_SETERS.put("JMSXDeliveryCount", new PropertySetter() {
            @Override
            public void set(StompJmsConnection connection, StompJmsMessage message, Object value) throws MessageFormatException {
                Integer rc = (Integer) TypeConversionSupport.convert(connection, value, Integer.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSXDeliveryCount cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setRedeliveryCounter(rc.intValue() - 1);
            }
        });

        JMS_PROPERTY_SETERS.put("JMSCorrelationID", new PropertySetter() {
            @Override
            public void set(StompJmsConnection connection, StompJmsMessage message, Object value) throws MessageFormatException {
                String rc = (String) TypeConversionSupport.convert(connection, value, String.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSCorrelationID cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setJMSCorrelationID(rc);
            }
        });
        JMS_PROPERTY_SETERS.put("JMSDeliveryMode", new PropertySetter() {
            @Override
            public void set(StompJmsConnection connection, StompJmsMessage message, Object value) throws MessageFormatException {
                Integer rc = (Integer) TypeConversionSupport.convert(connection, value, Integer.class);
                if (rc == null) {
                    Boolean bool = (Boolean) TypeConversionSupport.convert(connection, value, Boolean.class);
                    if (bool == null) {
                        throw new MessageFormatException("Property JMSDeliveryMode cannot be set from a " + value.getClass().getName() + ".");
                    }
                    rc = bool.booleanValue() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
                }
                message.setJMSDeliveryMode(rc);
            }
        });
        JMS_PROPERTY_SETERS.put("JMSExpiration", new PropertySetter() {
            @Override
            public void set(StompJmsConnection connection, StompJmsMessage message, Object value) throws MessageFormatException {
                Long rc = (Long) TypeConversionSupport.convert(connection, value, Long.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSExpiration cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setJMSExpiration(rc.longValue());
            }
        });
        JMS_PROPERTY_SETERS.put("JMSPriority", new PropertySetter() {
            @Override
            public void set(StompJmsConnection connection, StompJmsMessage message, Object value) throws MessageFormatException {
                Integer rc = (Integer) TypeConversionSupport.convert(connection, value, Integer.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSPriority cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setJMSPriority(rc.intValue());
            }
        });
        JMS_PROPERTY_SETERS.put("JMSRedelivered", new PropertySetter() {
            @Override
            public void set(StompJmsConnection connection, StompJmsMessage message, Object value) throws MessageFormatException {
                Boolean rc = (Boolean) TypeConversionSupport.convert(connection, value, Boolean.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSRedelivered cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setJMSRedelivered(rc.booleanValue());
            }
        });
        JMS_PROPERTY_SETERS.put("JMSReplyTo", new PropertySetter() {
            @Override
            public void set(StompJmsConnection connection, StompJmsMessage message, Object value) throws MessageFormatException {
                StompJmsDestination rc = (StompJmsDestination) TypeConversionSupport.convert(connection, value, StompJmsDestination.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSReplyTo cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setJMSReplyTo(rc);
            }
        });
        JMS_PROPERTY_SETERS.put("JMSTimestamp", new PropertySetter() {
            @Override
            public void set(StompJmsConnection connection, StompJmsMessage message, Object value) throws MessageFormatException {
                Long rc = (Long) TypeConversionSupport.convert(connection, value, Long.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSTimestamp cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setJMSTimestamp(rc.longValue());
            }
        });
        JMS_PROPERTY_SETERS.put("JMSType", new PropertySetter() {
            @Override
            public void set(StompJmsConnection connection, StompJmsMessage message, Object value) throws MessageFormatException {
                String rc = (String) TypeConversionSupport.convert(connection, value, String.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSType cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setJMSType(rc);
            }
        });
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        setObjectProperty(name, value, true);
    }

    public void setObjectProperty(String name, Object value, boolean checkReadOnly) throws JMSException {

        if (checkReadOnly) {
            checkReadOnlyProperties();
        }
        if (name == null || name.equals("")) {
            throw new IllegalArgumentException("Property name cannot be empty or null");
        }

        checkValidObject(value);
        PropertySetter setter = JMS_PROPERTY_SETERS.get(name);

        if (setter != null && value != null) {
            setter.set(connection, this, value);
        } else {
            this.setProperty(name, value);
        }
    }

    public void setProperties(Map<String, Object> properties) throws JMSException {
        for (Iterator<Map.Entry<String, Object>> iter = properties.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<String, Object> entry = iter.next();
            setObjectProperty(entry.getKey(), entry.getValue());
        }
    }

    protected void checkValidObject(Object value) throws MessageFormatException {

        boolean valid = value instanceof Boolean || value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long;
        valid = valid || value instanceof Float || value instanceof Double || value instanceof Character
                || value instanceof String || value == null;

        if (!valid) {

            throw new MessageFormatException(
                    "Only objectified primitive objects and String types are allowed but was: " + value + " type: "
                            + value.getClass());

        }
    }


    @Override
    public Object getObjectProperty(String name) throws JMSException {
        if (name == null) {
            throw new NullPointerException("Property name cannot be null");
        }

        // PropertyExpression handles converting message headers to properties.
        PropertyExpression expression = new PropertyExpression(name);
        return expression.evaluate(this);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            return false;
        }
        Boolean rc = (Boolean) TypeConversionSupport.convert(connection, value, Boolean.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a boolean");
        }
        return rc.booleanValue();
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Byte rc = (Byte) TypeConversionSupport.convert(connection, value, Byte.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a byte");
        }
        return rc.byteValue();
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Short rc = (Short) TypeConversionSupport.convert(connection, value, Short.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a short");
        }
        return rc.shortValue();
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Integer rc = (Integer) TypeConversionSupport.convert(connection, value, Integer.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as an integer");
        }
        return rc.intValue();
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Long rc = (Long) TypeConversionSupport.convert(connection, value, Long.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a long");
        }
        return rc.longValue();
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        }
        Float rc = (Float) TypeConversionSupport.convert(connection, value, Float.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a float");
        }
        return rc.floatValue();
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        }
        Double rc = (Double) TypeConversionSupport.convert(connection, value, Double.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a double");
        }
        return rc.doubleValue();
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            return null;
        }
        String rc = (String) TypeConversionSupport.convert(connection, value, String.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a String");
        }
        return rc;
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        setBooleanProperty(name, value, true);
    }

    public void setBooleanProperty(String name, boolean value, boolean checkReadOnly) throws JMSException {
        setObjectProperty(name, Boolean.valueOf(value), checkReadOnly);
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        setObjectProperty(name, Byte.valueOf(value));
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        setObjectProperty(name, Short.valueOf(value));
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        setObjectProperty(name, Integer.valueOf(value));
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        setObjectProperty(name, Long.valueOf(value));
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        setObjectProperty(name, new Float(value));
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        setObjectProperty(name, new Double(value));
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        setObjectProperty(name, value);
    }

    private void checkReadOnlyProperties() throws MessageNotWriteableException {
        if (readOnlyProperties) {
            throw new MessageNotWriteableException("Message properties are read-only");
        }
    }

    protected void checkReadOnlyBody() throws MessageNotWriteableException {
        if (readOnlyBody) {
            throw new MessageNotWriteableException("Message body is read-only");
        }
    }

    public Callable<Void> getAcknowledgeCallback() {
        return acknowledgeCallback;
    }

    public void setAcknowledgeCallback(Callable<Void> acknowledgeCallback) {
        this.acknowledgeCallback = acknowledgeCallback;
    }

    /**
     * Send operation event listener. Used to get the message ready to be sent.
     *
     * @throws JMSException
     */
    public void onSend() throws JMSException {
        setReadOnlyBody(true);
        setReadOnlyProperties(true);
        storeContent();
    }

    /**
     * serialize the payload
     *
     * @throws JMSException
     */
    public void storeContent() throws JMSException {
    }

    /**
     * @return the consumerId
     */
    public AsciiBuffer getConsumerId() {
        return getHeaderMap().get(SUBSCRIPTION);
    }

    protected Map<AsciiBuffer, AsciiBuffer> getHeaderMap() {
        return this.frame.headerMap(REVERSED_HEADER_NAMES);
    }

    /**
     * @return the transactionId
     */
    public AsciiBuffer getTransactionId() {
        return this.transactionId;
    }

    /**
     * @param transactionId the transactionId to set
     */
    public void setTransactionId(AsciiBuffer transactionId) {
        this.transactionId = transactionId;
    }

    public StompJmsConnection getConnection() {
        return connection;
    }

    public void setConnection(StompJmsConnection connection) {
        this.connection = connection;
    }
}
