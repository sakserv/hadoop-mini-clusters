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
package com.github.sakserv.minicluster.kafka;

import com.github.sakserv.minicluster.datetime.GenerateRandomDay;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaTestProducer {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTestProducer.class);

    private String kafkaHostname;
    private Integer kafkaPort;
    private String topic;
    private Integer messageCount;
    
    private KafkaTestProducer(Builder builder) {
        this.kafkaHostname = builder.kafkaHostname;
        this.kafkaPort = builder.kafkaPort;
        this.topic = builder.topic;
        this.messageCount = builder.messageCount;
    }
    
    public String getKafkaHostname() {
        return kafkaHostname;
    }
    
    public Integer getKafkaPort() {
        return kafkaPort;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public Integer getMessageCount() {
        return messageCount;
    }
    
    public static class Builder {
        private String kafkaHostname;
        private Integer kafkaPort;
        private String topic;
        private Integer messageCount;
        
        public Builder setKafkaHostname(String kafkaHostname) {
            this.kafkaHostname = kafkaHostname;
            return this;
        }
        
        public Builder setKafkaPort(Integer kafkaPort) {
            this.kafkaPort = kafkaPort;
            return this;
        }
        
        public Builder setTopic(String topic) {
            this.topic = topic;
            return this;
        }
        
        public Builder setMessageCount(Integer messageCount) {
            this.messageCount = messageCount;
            return this;
        }
        
        public KafkaTestProducer build() {
            KafkaTestProducer kafkaTestProducer = new KafkaTestProducer(this);
            return kafkaTestProducer;
        }
        
    }
    
    public void produceMessages() {
        Properties props = new Properties();
        props.put("metadata.broker.list", getKafkaHostname() + ":" + getKafkaPort());
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        // Send 10 messages to the local kafka server:
        LOG.info("KAFKA: Preparing To Send " + messageCount + " Initial Messages");
        for (int i=0; i<messageCount; i++){

            // Create the JSON object
            JSONObject obj = new JSONObject();
            try {
                obj.put("id", String.valueOf(i));
                obj.put("msg", "test-message" + 1);
                obj.put("dt", GenerateRandomDay.genRandomDay());
            } catch(JSONException e) {
                e.printStackTrace();
            }
            String payload = obj.toString();

            KeyedMessage<String, String> data = new KeyedMessage<String, String>(getTopic(), null, payload);
            producer.send(data);
            LOG.info("Sent message: " + data.toString());
        }
        LOG.info("KAFKA: Initial Messages Sent");
        
        producer.close();
    }
    
}
