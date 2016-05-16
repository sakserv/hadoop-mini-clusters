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
package com.github.sakserv.minicluster.kafka.producer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.datatime.GenerateRandomDay;

public class KafkaSimpleTestProducer {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSimpleTestProducer.class);

    private String kafkaHostname;
    private Integer kafkaPort;
    private String topic;
    private Integer messageCount;

    private KafkaSimpleTestProducer(Builder builder) {
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

        public KafkaSimpleTestProducer build() {
            KafkaSimpleTestProducer kafkaSimpleTestProducer = new KafkaSimpleTestProducer(this);
            return kafkaSimpleTestProducer;
        }

    }

    public Map<String, Object> createConfig() {
        Map<String, Object> config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaHostname() + ":" + getKafkaPort());
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return config;
    }

    public void produceMessages() {

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(createConfig());

        int count = 0;
        while(count < getMessageCount()) {

            // Create the JSON object
            JSONObject obj = new JSONObject();
            try {
                obj.put("id", String.valueOf(count));
                obj.put("msg", "test-message" + 1);
                obj.put("dt", GenerateRandomDay.genRandomDay());
            } catch(JSONException e) {
                e.printStackTrace();
            }
            String payload = obj.toString();

            producer.send(new ProducerRecord<String, String>(getTopic(), payload));
            LOG.info("Sent message: {}", payload.toString());
            count++;
        }
    }

}
