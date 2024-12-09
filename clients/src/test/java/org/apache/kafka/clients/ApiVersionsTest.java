/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Locale;
import java.util.Properties;


import com.azul.crs.json.DummyJSONSerializer;


import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.ResponseHeader;
import org.junit.Test;

public class ApiVersionsTest {

    @Test
    public void testMaxUsableProduceMagic() {
        ApiVersions apiVersions = new ApiVersions();
        assertEquals(RecordBatch.CURRENT_MAGIC_VALUE, apiVersions.maxUsableProduceMagic());

        apiVersions.update("0", NodeApiVersions.create());
        assertEquals(RecordBatch.CURRENT_MAGIC_VALUE, apiVersions.maxUsableProduceMagic());

        apiVersions.update("1", NodeApiVersions.create(Collections.singleton(
                new ApiVersionsResponse.ApiVersion(ApiKeys.PRODUCE.id, (short) 0, (short) 2))));
        assertEquals(RecordBatch.MAGIC_VALUE_V1, apiVersions.maxUsableProduceMagic());

        apiVersions.remove("1");
        assertEquals(RecordBatch.CURRENT_MAGIC_VALUE, apiVersions.maxUsableProduceMagic());
    }

    @Test
    public void testSer() {
        ResponseHeader header = new ResponseHeader(0);
        ByteBuffer buffer = ByteBuffer.allocate(10);
        header.toStruct().writeTo(buffer);
        buffer.flip();
        while (buffer.hasRemaining()) {
            System.out.printf("-----%02X ", buffer.get());
        }

    }

    @Test
    public void testStoneMQ() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Faker faker = new Faker(Locale.CHINA);





        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 500; i++) {
            long userId=faker.number().randomNumber();
            long orderId=faker.number().randomNumber();
            long productId=faker.number().randomNumber();
            String userName=faker.name().username();
            String address=faker.address().fullAddress();
            Order order=new Order(userId,orderId,productId,userName,address);
            DummyJSONSerializer serializer= new DummyJSONSerializer();
            String msg_value= serializer.serialize(order);
            int size=0; boolean simple=true;

            if(simple){
                size=1;
            }
            else
                size=msg_value.length()+1;
            StringBuilder builder=new StringBuilder(size);
            builder.append(i);
//            builder.append("-");
//            builder.append(msg_value);
            producer.send(new ProducerRecord<String, String>("topic_a",builder.toString()));
        }

        producer.close();

    }
    private class Order{
        long userId;
        long orderId;
        long productId;
        String username;
        String address;

        public Order(long userId,long orderId,long productId,String username,String address){
            this.userId=userId;
            this.orderId=orderId;
            this.productId=productId;
            this.username=username;
            this.address = address;
        }
    }

}
