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
#include <librdkafka/src-cpp/rdkafkacpp.h>

#include "Component.h"

struct KafkaSettings{
    std::string Key;
    std::string Value;
};

static std::string logsReportFileName;

class clDeliveryReportCb : public RdKafka::DeliveryReportCb {
  public:
    void dr_cb (RdKafka::Message &message);
};

class SimpleKafka1C final : public Component {
public:
    const char *Version = u8"1.0.1";

    SimpleKafka1C();

private:

    RdKafka::Producer *hProducer;
    RdKafka::KafkaConsumer *hConsumer;

    long currentOffsetValue;
    int currentPartition;
    int32_t waitMessageTimeout;

    std::string topicName;
    std::vector<KafkaSettings> settings;
    std::string extensionName() override;

    // reports filename
    void setLogsReportFileName(const variant_t &filename);

    // parameters set 
    // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    void setParameter(const variant_t &key, const variant_t &value);

    // producer
    bool initProducer(const variant_t &brokers, const variant_t &topic);
    bool produce(const variant_t &msg, const variant_t &partition, const variant_t &key, const variant_t &heads);
    void stopProducer();

    // consumer
    bool initConsumer(const variant_t &brokers, const variant_t &topic);
    variant_t consume();
    bool commitOffset(const variant_t &offset);
    long currentOffset();
    void stopConsumer();
    void setWaitingTimeout(const variant_t &timeout);

    // default component implementation
    void message(const variant_t &msg);
    void sleep(const variant_t &delay);

    class clEventCb : public RdKafka::EventCb {
      public:
        void event_cb (RdKafka::Event &event);
    };

    class clRebalanceCb : public RdKafka::RebalanceCb
    {

      public:
        void rebalance_cb(RdKafka::KafkaConsumer *consumer,
                          RdKafka::ErrorCode err,
                          std::vector<RdKafka::TopicPartition *> &partitions);
    };
};

#endif //SIMPLEKAFKA1C_H
