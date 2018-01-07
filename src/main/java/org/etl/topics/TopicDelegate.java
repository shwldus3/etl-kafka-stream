package org.etl.topics;

public class TopicDelegate {
  private TopicService topicInfo;

  public TopicDelegate(String topicName, String className, String messageType) {
    if (messageType.equals("JSON")) {
      try {
        topicInfo = JsonTopic.createJsonTopic(topicName, className);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
    } else {
      //TODO: You can define and use other types of message formats.
    }
  }

  public TopicService getTopicInfo() {
    return topicInfo;
  }
}
