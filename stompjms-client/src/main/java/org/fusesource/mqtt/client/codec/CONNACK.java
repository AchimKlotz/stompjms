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

import org.fusesource.hawtbuf.*;

import java.io.IOException;
import java.net.ProtocolException;
import static org.fusesource.mqtt.client.codec.CommandSupport.*;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class CONNACK implements Command {

    public static final byte TYPE = 2;

    public static enum Code {
        CONNECTION_ACCEPTED,
        CONNECTION_REFUSED_UNACCEPTED_PROTOCOL_VERSION,
        CONNECTION_REFUSED_IDENTIFIER_REJECTED,
        CONNECTION_REFUSED_SERVER_UNAVAILABLE,
        CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD,
        CONNECTION_REFUSED_NOT_AUTHORIZED;
    }

    private Code code = Code.CONNECTION_ACCEPTED;
    
    public byte getType() {
        return TYPE;
    }

    public CONNACK decode(MQTTFrame frame) throws ProtocolException {
        assert(frame.buffers.length == 1);
        DataByteArrayInputStream is = new DataByteArrayInputStream(frame.buffers[0]);
        is.skip(1);
        byte c = is.readByte();
        if( c >= Code.values().length ) {
            throw new ProtocolException("Invalid CONNACK encoding");
        }
        code = Code.values()[c];
        return this;
    }
    
    public MQTTFrame encode() {
        try {
            DataByteArrayOutputStream os = new DataByteArrayOutputStream(2);
            os.writeByte(0);
            os.writeByte(code.ordinal());

            MQTTFrame frame = new MQTTFrame();
            frame.commandType(TYPE);
            return frame.buffer(os.toBuffer());
        } catch (IOException e) {
            throw new RuntimeException("The impossible happened");
        }
    }

    public Code code() {
        return code;
    }

    public CONNACK code(Code code) {
        this.code = code;
        return this;
    }

    @Override
    public String toString() {
        return "CONNACK{" +
                "code=" + code +
                '}';
    }
}
