/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.stomp.jms.jndi;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.naming.Context;
import javax.naming.NamingException;

/**
 * This implementation of <CODE>InitialContextFactory</CODE> should be used
 * when StompJms is used as WebSphere Generic JMS Provider. It is proved that it
 * works on WebSphere 5.1. The reason for using this class is that custom
 * property defined for Generic JMS Provider are passed to InitialContextFactory
 * only if it begins with java.naming or javax.naming prefix. Additionally
 * provider url for the JMS provider can not contain ',' character that is
 * necessary when the list of nodes is provided. So the role of this class is to
 * transform properties before passing it to
 * <CODE>ActiveMQInitialContextFactory</CODE>.
 *
 * @author Pawel Tucholski
 */
public class WasStompJmsInitialContextFactory extends StompJmsInitialContextFactory {

    /**
     * @see javax.naming.spi.InitialContextFactory#getInitialContext(java.util.Hashtable)
     */
    @SuppressWarnings("unchecked")
    @Override
    public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {

        return super.getInitialContext(transformEnvironment((Hashtable<String, String>) environment));
    }

    /**
     * Performs following transformation of properties:
     * <ul>
     * <li>(java.naming.queue.xxx.yyy,value)=>(queue.xxx/yyy,value)
     * <li>(java.naming.topic.xxx.yyy,value)=>(topic.xxx/yyy,value)
     * <li>(java.naming.connectionFactoryNames,value)=>(connectionFactoryNames,value)
     * <li>(java.naming.provider.url,url1;url2)=>java.naming.provider.url,url1,url1)
     * <ul>
     *
     * @param environment properties for transformation
     * @return environment after transformation
     */
    protected Hashtable<String, Object> transformEnvironment(Hashtable<String, String> environment) {

        Hashtable<String, Object> environment1 = new Hashtable<>();

        Iterator<Map.Entry<String, String>> it = environment.entrySet().iterator();

        while (it.hasNext()) {
            Entry<String, String> entry = it.next();
            String key = entry.getKey();
            String value = entry.getValue();

            if (key.startsWith("java.naming.queue")) {
                String key1 = key.substring("java.naming.queue.".length());
                key1 = key1.replace('.', '/');
                environment1.put("queue." + key1, value);
            } else if (key.startsWith("java.naming.topic")) {
                String key1 = key.substring("java.naming.topic.".length());
                key1 = key1.replace('.', '/');
                environment1.put("topic." + key1, value);
            } else if (key.startsWith("java.naming.connectionFactoryNames")) {
                String key1 = key.substring("java.naming.".length());
                environment1.put(key1, value);
            } else if (key.startsWith("java.naming.connection")) {
                String key1 = key.substring("java.naming.".length());
                environment1.put(key1, value);
            } else if (key.startsWith(Context.PROVIDER_URL)) {
                // websphere administration console does not exept , character
                // in provider url, so ; must be used
                // all ; to ,
                value = value.replace(';', ',');
                environment1.put(Context.PROVIDER_URL, value);
            } else {
                environment1.put(key, value);
            }
        }

        return environment1;
    }
}
