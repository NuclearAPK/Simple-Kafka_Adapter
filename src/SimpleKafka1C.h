#ifndef SIMPLEKAFKA1C_H
#define SIMPLEKAFKA1C_H

#include <chrono>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <fstream>
#include <map>
#ifdef __linux__
#include <librdkafka/rdkafkacpp.h>
#else 
#include <librdkafka/src-cpp/rdkafkacpp.h>
#endif

#include "Component.h"

class SimpleKafka1C final : public Component {
public:
    const char *Version = u8"1.2.2";

    SimpleKafka1C();

private:

    // property
    std::shared_ptr<variant_t> logDirectory;
    std::shared_ptr<variant_t> formatLogFiles;

    RdKafka::Producer *hProducer;
    RdKafka::KafkaConsumer *hConsumer;

    int32_t waitMessageTimeout;
    unsigned pid;

    std::string consumerLogName; 
    std::string producerLogName;
    std::string dumpLogName;
    std::string statLogName; 

    std::string extensionName() override;

    // parameters set 
    // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    void setParameter(const variant_t &key, const variant_t &value);
    std::string clientID();

    // producer
    bool initProducer(const variant_t &brokers);
    variant_t produce(const variant_t &msg, const variant_t &topicName, const variant_t &partition, const variant_t &key, const variant_t &heads);
    variant_t produceWithWaitResult(const variant_t &msg, const variant_t &topicName, const variant_t &partition, const variant_t &key, const variant_t &heads);
    void stopProducer();

    // consumer
    bool initConsumer(const variant_t &brokers, const variant_t &topic);
    variant_t consume();
    bool commitOffset(const variant_t &topicName, const variant_t &offset, const variant_t &partition);
    void setReadingPosition(const variant_t &topicName, const variant_t &offset, const variant_t &partition);
    void stopConsumer();
    void setWaitingTimeout(const variant_t &timeout);

    // default component implementation
    void message(const variant_t &msg);
    void sleep(const variant_t &delay);

    struct KafkaSettings{
      std::string Key;
      std::string Value;
    };

    class clEventCb : public RdKafka::EventCb {
      public:
        unsigned pid;        
        char *formatLogFiles;
        std::string logDir = "";
        std::string consumerLogName;
        std::string statLogName;
        std::string clientid;

        void event_cb (RdKafka::Event &event);
    };

    class clDeliveryReportCb : public RdKafka::DeliveryReportCb {
      public:
        unsigned pid;
        bool delivered = false;
        char *formatLogFiles;
        std::string logDir = "";
        std::string producerLogName;
        std::string clientid;      

        void dr_cb(RdKafka::Message &message);
    };

    class clRebalanceCb : public RdKafka::RebalanceCb
    {
      public:

        std::string assignTopic = "";
        int32_t assignOffset = -1;
        int assignPartition = 0;

        void rebalance_cb(RdKafka::KafkaConsumer *consumer,
                          RdKafka::ErrorCode err,
                          std::vector<RdKafka::TopicPartition *> &partitions);
    };

    std::vector<KafkaSettings> settings;

    clEventCb cl_event_cb;
    clDeliveryReportCb cl_dr_cb;
    clRebalanceCb cl_rebalance_cb;
};

#endif //SIMPLEKAFKA1C_H
