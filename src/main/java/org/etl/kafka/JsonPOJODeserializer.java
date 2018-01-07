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

package org.etl.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonPOJODeserializer<T> implements Deserializer<T> {
  private ObjectMapper objectMapper = new ObjectMapper();
  private Logger logger = LoggerFactory.getLogger(this.getClass());

  private Class<T> tClass;

  /**
  * Default constructor needed by Kafka
  */
  public JsonPOJODeserializer() {
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
        tClass = (Class<T>) props.get("JsonPOJOClass");
    }

  @Override
  public T deserialize(String topic, byte[] bytes) {
    if (bytes == null)
      return null;

    T data;
    try {
      data = objectMapper.readValue(bytes, tClass);
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("JSON 형태의 메세지가 아닙니다. 다시 시도해주세요");
      return null;
    }

    return data;
  }

  @Override
  public void close() {
  }
}
