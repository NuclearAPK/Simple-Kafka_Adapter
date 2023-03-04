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

class clDeliveryReportCb : public RdKafka::DeliveryReportCb {
  public:
    void dr_cb (RdKafka::Message &message);
};

class clEventCb : public RdKafka::EventCb {
  public:
    void event_cb (RdKafka::Event &event) {

    // if(eventReportFileName.empty()){
    //     return;
    // }
    // std::ofstream file(eventReportFileName,  std::ios_base::app);
    //std::ofstream file("d:/temp/_er.txt",  std::ios_base::app);
    //file << "aaaaa" << std::endl;

//    switch (event.type())
//    {
//      case RdKafka::Event::EVENT_ERROR:
//
//        file << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
//            event.str() << std::endl;
//        break;
//
//      case RdKafka::Event::EVENT_STATS:
//        file << "\"STATS\": " << event.str() << std::endl;
//        break;
//
//      default:
//        file << "EVENT " << event.type() <<
//            " (" << RdKafka::err2str(event.err()) << "): " <<
//            event.str() << std::endl;
//        break;
//    }
    //file.close();
  }
};


class SimpleKafka1C final : public Component {
public:
    const char *Version = u8"1.0.0";

    SimpleKafka1C();

private:

    RdKafka::Producer *hProducer;
    RdKafka::Consumer *hConsumer;
    RdKafka::Topic *hTopic;

    // event callback
    //clEventCb ex_event_cb;
    // delivery callback
    //clDeliveryReportCb ex_dr_cb;

    int32_t hPartition = 0;

    std::string deliveryReportFileName;
    std::string eventReportFileName;

    std::string topicName = "";
    std::vector<KafkaSettings> settings;
    std::map<std::string, int64_t> offsetSettings;

    std::string extensionName() override;

    // reports filename
    //void setDeliveryReportFileName(const variant_t &filename);
    //void setEventReportFileName(const variant_t &filename);

    // parameters set 
    // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    void setParameter(const variant_t &key, const variant_t &value);

    // producer
    bool initProducer(const variant_t &brokers, const variant_t &topic);
    bool produce(const variant_t &msg, const variant_t &partition, const variant_t &key, const variant_t &heads);
    void stopProducer();

    // consumer
    bool initConsumer(const variant_t &brokers, const variant_t &topic, const variant_t &offset, const variant_t &partition);
    variant_t consume(const variant_t &timeout);
    void stopConsumer();

    // default component implementation
    void message(const variant_t &msg);
    void sleep(const variant_t &delay);
};

#endif //SIMPLEKAFKA1C_H
