/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.github.sakserv.minicluster;

import com.github.sakserv.datetime.GenerateRandomDay;
import com.github.sakserv.kafka.KafkaTestConsumer;
import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.config.PropertyParser;
import com.github.sakserv.minicluster.impl.KafkaLocalBroker;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.common.ErrorMapping;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndOffset;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KafkaLocalBrokerTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KafkaLocalBrokerTest.class);

    // Setup the property parser
    private static PropertyParser propertyParser;
    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
        } catch(IOException e) {
            LOG.error("Unable to load property file: " + propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }

    private static final String TEST_TOPIC = "test_topic";
    
    
    

    ZookeeperLocalCluster zookeeperLocalCluster;
    KafkaLocalBroker kafkaLocalBroker;

   

    @Before
    public void setUp() throws IOException {
        zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setTempDir(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .build();
        zookeeperLocalCluster.start();

        kafkaLocalBroker = new KafkaLocalBroker(TEST_TOPIC);
        kafkaLocalBroker.start();

    }

    @After
    public void tearDown() {

        kafkaLocalBroker.stop(true);
        zookeeperLocalCluster.stop();
    }

    @Test
    public void testKafkaLocalBroker() throws Exception {

        //
        // Add Producer properties and create the Producer
        //
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:" + kafkaLocalBroker.getPort());
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        // Send 10 messages to the local kafka server:
        LOG.info("KAFKA: Preparing To Send 10 Initial Messages");
        for (int i=0; i<10; i++){

            // Create the JSON object
            JSONObject obj = new JSONObject();
            try {
                obj.put("id", String.valueOf(i));
                obj.put("msg", "test-message" + 1);
                obj.put("dt", GenerateRandomDay.genRandomDay());
            } catch(JSONException e) {
                e.printStackTrace();
                System.exit(3);
            }
            String payload = obj.toString();

            KeyedMessage<String, String> data = new KeyedMessage<String, String>(TEST_TOPIC, null, payload);
            producer.send(data);
            LOG.info("Sent message: " + data.toString());
        }
        LOG.info("KAFKA: Initial Messages Sent");

        // Stop the producer
        producer.close();

        try {
            Thread.sleep(5000);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }

        // Consumer
        List<String> seeds = new ArrayList<String>();
        seeds.add("localhost");
        KafkaTestConsumer kafkaTestConsumer = new KafkaTestConsumer();
        kafkaTestConsumer.consumeMessages(10, TEST_TOPIC, 0, seeds, kafkaLocalBroker.getPort());
        assertEquals(10, kafkaTestConsumer.getNumRead());

        try {
            Thread.sleep(5000);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }

    }

}
