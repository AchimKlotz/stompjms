/**
 * Copyright (C) 2010-2011, FuseSource Corp.  All rights reserved.
 *
 *     http://fusesource.com
 *
 * The software in this package is published under the terms of the
 * CDDL license a copy of which has been included with this distribution
 * in the license.txt file.
 */

package org.fusesource.mqtt.client.codec;

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.DataByteArrayInputStream;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.QoS;

import java.io.IOException;
import java.net.ProtocolException;
import static org.fusesource.mqtt.client.codec.CommandSupport.*;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class PUBLISH extends CommandSupport.HeaderBase implements Command, Acked {

    public static final byte TYPE = 3;

    private UTF8Buffer topicName;
    private short messageId;
    private Buffer payload;

    public PUBLISH() {
        qos(QoS.AT_LEAST_ONCE);
    }

    public byte getType() {
        return TYPE;
    }

    public PUBLISH decode(MQTTFrame frame) throws ProtocolException {
        assert(frame.buffers.length == 1);
        header(frame.header());

        DataByteArrayInputStream is = new DataByteArrayInputStream(frame.buffers[0]);
        topicName = CommandSupport.readUTF(is);
        
        QoS qos = qos();
        if(qos != QoS.AT_MOST_ONCE) {
            messageId = is.readShort();
        }
        payload = is.readBuffer(is.available());
        return this;
    }
    
    public MQTTFrame encode() {
        try {
            DataByteArrayOutputStream variableHeader = new DataByteArrayOutputStream();
            CommandSupport.writeUTF(variableHeader, topicName);
            QoS qos = qos();
            if(qos != QoS.AT_MOST_ONCE) {
                variableHeader.writeShort(messageId);
            }
            MQTTFrame frame = new MQTTFrame();
            frame.header(header());
            frame.commandType(TYPE);
            if(payload==null || payload.length==0) {
                frame.buffer(variableHeader.toBuffer());
            } else {
                frame.buffers(variableHeader.toBuffer(), payload);
            }
            return frame;
        } catch (IOException e) {
            throw new RuntimeException("The impossible happened");
        }
    }

    @Override
    public boolean dup() {
        return super.dup();
    }

    @Override
    public PUBLISH dup(boolean dup) {
        return (PUBLISH) super.dup(dup);
    }

    @Override
    public QoS qos() {
        return super.qos();
    }

    @Override
    public PUBLISH qos(QoS qos) {
        return (PUBLISH) super.qos(qos);
    }

    @Override
    public boolean retain() {
        return super.retain();
    }

    @Override
    public PUBLISH retain(boolean retain) {
        return (PUBLISH) super.retain(retain);
    }

    public short messageId() {
        return messageId;
    }

    public PUBLISH messageId(short messageId) {
        this.messageId = messageId;
        return this;
    }

    public Buffer payload() {
        return payload;
    }

    public PUBLISH setPayload(Buffer payload) {
        this.payload = payload;
        return this;
    }

    public UTF8Buffer topicName() {
        return topicName;
    }

    public PUBLISH topicName(UTF8Buffer topicName) {
        this.topicName = topicName;
        return this;
    }

    @Override
    public String toString() {
        return "PUBLISH{" +
                "dup=" + dup() +
                ", qos=" + qos() +
                ", retain=" + retain() +
                ", messageId=" + messageId +
                ", topicName=" + topicName +
                ", payload=" + payload +
                '}';
    }
}
