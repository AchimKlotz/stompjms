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

import org.fusesource.hawtbuf.DataByteArrayInputStream;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;
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
public class PUBREL extends CommandSupport.HeaderBase implements Command, Acked {

    public static final byte TYPE = 6;

    private short messageId;

    public byte getType() {
        return TYPE;
    }
    
    public PUBREL() {
        qos(QoS.AT_LEAST_ONCE);
    }

    public PUBREL decode(MQTTFrame frame) throws ProtocolException {
        assert(frame.buffers.length == 1);
        header(frame.header());
        DataByteArrayInputStream is = new DataByteArrayInputStream(frame.buffers[0]);
        messageId = is.readShort();
        return this;
    }
    
    public MQTTFrame encode() {
        try {
            DataByteArrayOutputStream os = new DataByteArrayOutputStream(2);
            os.writeShort(messageId);

            MQTTFrame frame = new MQTTFrame();
            frame.header(header());
            frame.commandType(TYPE);
            return frame.buffer(os.toBuffer());
        } catch (IOException e) {
            throw new RuntimeException("The impossible happened");
        }
    }


    @Override
    public boolean dup() {
        return super.dup();
    }

    @Override
    public PUBREL dup(boolean dup) {
        return (PUBREL) super.dup(dup);
    }

    @Override
    public QoS qos() {
        return super.qos();
    }

    public short messageId() {
        return messageId;
    }

    public PUBREL messageId(short messageId) {
        this.messageId = messageId;
        return this;
    }

    @Override
    public String toString() {
        return "PUBREL{" +
                "dup=" + dup() +
                ", qos=" + qos() +
                ", messageId=" + messageId +
                '}';
    }
}
