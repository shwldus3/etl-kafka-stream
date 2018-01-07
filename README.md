

# etl-kafka-stream

### 01. Requirements

1. Maven Project
2. POJO 
3. Clean Code (OOP)
4. Kafka Stream
5. Mysql (RDB)

### 02. TO-DO

> Create a Java program that uses the framework to read data from the Kafka topic, remove data redundancy within the 10-minute time window, and finally store the data in the MySql table.

### 03. Setting to run this project

1. **Create java file in schema folder**
   - This is the same as the Json data type and Mysql Table schema that come into Kafka Topic
   - Mysql

     ```mysql
     CREATE TABLE Event (
     	event_id BIGINT NOT NULL,
     	event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
     	service_code VARCHAR(5),
     	event_context VARCHAR(100)
     );
     ```

   - Event.java

     ```java
     public class Event {
     	public long event_id;
     	public Timestamp event_timestamp;
     	public String service_code;
     	public String event_context;

     	public String toString() {
     		return "Data => " + event_id + " | " + event_timestamp + " | " + service_code + " | " + event_context;
     	}
     }
     ```

2. **Set config.properties**

   - Mysql, Kafka information can be set.

   - You can change properties and App.java file according to the topic that the producer will send.

     ```properties
     kafka.applicationId=etl-kafka-stream
     kafka.brokerList=localhost:9092

     kafka.task1.topic=streams-event-1
     kafka.task2.topic=streams-event-2

     mysql.connectionString=jdbc:mysql://127.0.0.1:3306/database?verifyServerCertificate=false&useSSL=false
     mysql.username=username
     mysql.password=password
     ```

3. **Run App.java**


