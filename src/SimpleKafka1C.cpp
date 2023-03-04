#include <boost/algorithm/string/split.hpp> // boost::algorithm::split
#include <boost/algorithm/string/classification.hpp> // boost::is_any_of
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "SimpleKafka1C.h"


char* slice(char*s, size_t from, size_t to)
{
    size_t j = 0;
    for(size_t i = from; i <= to; ++i)
        s[j++] = s[i];
    s[j] = 0;
    return s;
};

// void clDeliveryReportCb::dr_cb(RdKafka::Message &message) {

//     // if(deliveryReportFileName.empty()){
//     //     return;
//     // }
//     // std::ofstream file(deliveryReportFileName,  std::ios_base::app);
//     std::ofstream file("d:/temp/_dr.txt",  std::ios_base::app);

//     if (message.err())
//       file << "Message delivery failed: " << message.errstr()
//                 << std::endl;
//     else
//       file << "Message delivered to topic " << message.topic_name()
//                 << " [" << message.partition() << "] at offset "
//                 << message.offset() << std::endl;

//     // file << "Message delivery for (" << message.len() << " bytes): " <<
//     //     message.errstr() << std::endl;
//     file.close();
// }

std::string SimpleKafka1C::extensionName() {
    return "SimpleKafka1C";
}

SimpleKafka1C::SimpleKafka1C() {

    AddProperty(L"Version", L"ВерсияКомпоненты", [&]() {
        auto s = std::string(Version);
        return std::make_shared<variant_t>(std::move(s));
    });

    //AddMethod(L"SetDeliveryReportFileName", L"УстановитьИмяФайлаОтчетаОбДоставке", this, &SimpleKafka1C::setDeliveryReportFileName);
    //AddMethod(L"SetEventReportFileName", L"УстановитьИмяФайлаСобытияСообщения", this, &SimpleKafka1C::setEventReportFileName);

    AddMethod(L"SetParameter", L"УстановитьПараметр", this, &SimpleKafka1C::setParameter);
    AddMethod(L"InitializeProducer", L"ИнициализироватьПродюсера", this, &SimpleKafka1C::initProducer);
    AddMethod(L"Produce", L"ОтправитьСообщение", this, &SimpleKafka1C::produce,
              {{1, -1}, {2, std::string("")}, {3, std::string("")}});
    AddMethod(L"StopProducer", L"ОстановитьПродюсера", this, &SimpleKafka1C::stopProducer);

    AddMethod(L"InitializeConsumer", L"ИнициализироватьКонсьюмера", this, &SimpleKafka1C::initConsumer, {{3, -1}});
    AddMethod(L"Consume", L"Слушать", this, &SimpleKafka1C::consume);
    AddMethod(L"StopConsumer", L"ОстановитьКонсьюмера", this, &SimpleKafka1C::stopConsumer);

    AddMethod(L"Message", L"Сообщить", this, &SimpleKafka1C::message);
    AddMethod(L"Sleep", L"Пауза", this, &SimpleKafka1C::sleep);

    // add parameters
    offsetSettings["OFFSET_BEGINNING"] = RdKafka::Topic::OFFSET_BEGINNING;
    offsetSettings["OFFSET_END"] = RdKafka::Topic::OFFSET_END;
    offsetSettings["OFFSET_STORED"] = RdKafka::Topic::OFFSET_STORED;
    offsetSettings["OFFSET_INVALID"] = RdKafka::Topic::OFFSET_INVALID;
}

// void SimpleKafka1C::setDeliveryReportFileName(const variant_t &filename){
//     deliveryReportFileName = std::get<std::string>(filename);    
// }

// void SimpleKafka1C::setEventReportFileName(const variant_t &filename){
//     //eventReportFileName = std::get<std::string>(filename); 
// }

void SimpleKafka1C::setParameter(const variant_t &key, const variant_t &value){
    settings.push_back({std::get<std::string>(key), std::get<std::string>(value)});
}

bool SimpleKafka1C::initProducer(const variant_t &brokers, const variant_t &topic){

    std::string msg_err = "";

    // if (!deliveryReportFileName.empty())
    // {
    //     plog::init(plog::info, const <char *>());     
    // }

    // debug
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    if (conf->set("metadata.broker.list", std::get<std::string>(brokers), msg_err) != RdKafka::Conf::CONF_OK){
        message(msg_err);
        return false;
    }

    for (int i = 0; i < settings.size(); i++){
        if (conf->set(settings[i].Key, settings[i].Value, msg_err) != RdKafka::Conf::CONF_OK){
            message(msg_err);
            return false;
        }
    }

    // conf->set("event_cb", &ex_event_cb, msg_err);
    // conf->set("dr_cb", &ex_dr_cb, msg_err);

    hProducer = RdKafka::Producer::create(conf, msg_err);
    if (!hProducer) {
        message(msg_err);
        return false;
    }

    topicName = std::get<std::string>(topic);
    delete conf;
    return true;
}

bool SimpleKafka1C::produce(const variant_t &msg, const variant_t &partition, const variant_t &key, const variant_t &heads) {

    if (!std::holds_alternative<std::string>(msg)) {
        throw std::runtime_error(u8"Неподдерживаемый тип данных сообщения");
    }

    if (!std::holds_alternative<int>(partition)) {
        throw std::runtime_error(u8"Неверный тип данных партиции");
    }

    std::string msg_err = "";

    std::vector<std::string> splitResult;
    boost::algorithm::split(splitResult, std::get<std::string>(heads), boost::is_any_of(";"));

    RdKafka::Headers *hdrs = RdKafka::Headers::create();

    for(std::string& s : splitResult) {
        std::vector<std::string> hKeyValue;
        boost::algorithm::split(hKeyValue, s, boost::is_any_of(","));
        if(hKeyValue.size() == 2)
            hdrs->add(hKeyValue[0], hKeyValue[1]);
    }

    retry:

    auto currentPartition = std::get<int>(partition);
    RdKafka::ErrorCode resp = hProducer->produce(
            topicName,
            currentPartition == -1 ? RdKafka::Topic::PARTITION_UA : currentPartition,
            RdKafka::Producer::RK_MSG_COPY,
            const_cast<char *>(std::get<std::string>(msg).c_str()),std::get<std::string>(msg).size(),
            /* Key */
            const_cast<char *>(std::get<std::string>(key).c_str()), std::get<std::string>(key).size(),
            /* Timestamp (defaults to current time) */
            0,
            /* Message headers, if any */
            hdrs,
            /* Per-message opaque value passed to
             * delivery report */
            NULL);

    if (resp != RdKafka::ERR_NO_ERROR){
        if (resp == RdKafka::ERR__QUEUE_FULL) {
            hProducer->poll(1000 /*block for max 1000ms*/);
            msg_err = "Достигнуто максимальное количество ожидающих сообщений:queue.buffering.max.message";
            message(msg_err);
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            goto retry;
        } else if(resp == RdKafka::ERR_MSG_SIZE_TOO_LARGE ){
            msg_err = "Размер сообщения превышает установленный максимальный размер:messages.max.bytes";
        } else if(resp == RdKafka::ERR__UNKNOWN_PARTITION ){
            msg_err = "Запрошенный partition неизвестен в кластере Kafka";
        } else if(resp == RdKafka::ERR__UNKNOWN_TOPIC  ){
            msg_err = "Указанная тема не найдена в кластере Kafka";          
        }           
        delete hdrs;    
    } 
    
    hProducer->poll(0);

    if (!msg_err.empty()){
        message(msg_err);
        return false;
    }

    return true;   
}

void SimpleKafka1C::stopProducer(){

    hProducer->flush(10 * 1000 /* wait for max 10 seconds */);
    delete hProducer;
}

bool SimpleKafka1C::initConsumer(const variant_t &brokers, const variant_t &topic, const variant_t &offset, const variant_t &partition){

    std::string msg_err = "";

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

    if (conf->set("metadata.broker.list", std::get<std::string>(brokers), msg_err) != RdKafka::Conf::CONF_OK){
        message(msg_err);
        return false;
    }

    for (int i = 0; i < settings.size(); i++){
        if (conf->set(settings[i].Key, settings[i].Value, msg_err) != RdKafka::Conf::CONF_OK){
            message(msg_err);
            return false;
        }
    }

    // conf->set("event_cb", &ex_event_cb, msg_err);
    // conf->set("dr_cb", &ex_dr_cb, msg_err);

    auto offset_param = offsetSettings.find(std::get<std::string>(offset))->second;
    
    hConsumer = RdKafka::Consumer::create(conf, msg_err);
    if (!hConsumer) {
        message(msg_err);
        return false;
    }
    delete conf;
    hTopic = RdKafka::Topic::create(hConsumer, std::get<std::string>(topic), tconf, msg_err);

    if (!hTopic) {
          message(msg_err);
          return false;
    }
    delete tconf;

    auto currentPartition = std::get<int>(partition);
    RdKafka::ErrorCode resp = hConsumer->start(hTopic, currentPartition == -1 ? RdKafka::Topic::PARTITION_UA : currentPartition, offset_param);

    if (resp != RdKafka::ERR_NO_ERROR) {
        msg_err = RdKafka::err2str(resp);
        message(msg_err);
        return false;
    }

    return true;
}

variant_t SimpleKafka1C::consume(const variant_t &timeout){
    
    //const RdKafka::Headers *headers;
    boost::property_tree::ptree jsonObj;

    RdKafka::Message *msg = hConsumer->consume(hTopic, hPartition, std::get<int32_t>(timeout));
    std::stringstream s;
    std::string emptystr = "";

    auto payload = static_cast<char *>(msg->payload());

    switch (msg->err()) {

        case RdKafka::ERR_NO_ERROR:

            if (msg->key()) {
                jsonObj.put("key", *msg->key());
            }
            jsonObj.put("offset", msg->offset());
            jsonObj.put("message", std::string(slice(payload, 0, msg->len())));

            //rd_kafka_message_t *ptr = msg->c_ptr();

            // headers = msg->headers();
            // if (headers) {

            //     //boost::property_tree::ptree childrenHeaders; 
            //     //boost::property_tree::ptree *child;

            //     std::vector<RdKafka::Headers::Header> hdrs = headers->get_all();
            //     for (size_t i = 0; i < hdrs.size(); i++) {
            //         const RdKafka::Headers::Header hdr = hdrs[i];

            //         if (hdr.value() != NULL){
            //             message(std::string((const char *)hdr.value()));
            //             // boost::property_tree::ptree *child = new boost::property_tree::ptree;
            //             // child->put("headerKey", hdr.key().c_str());
            //             // child->put("headerValue", (const char *)hdr.value());

            //             // childrenHeaders.push_back(std::make_pair("", *child));
            //         }
            //     }
            //     //jsonObj.add_child("headers", childrenHeaders);
            // }

            break;

        default:
            delete msg;
            return emptystr;
    }

    boost::property_tree::write_json(s, jsonObj, true);

    delete msg;
    return s.str();

 }

void SimpleKafka1C::stopConsumer(){

#ifndef _WIN32
    alarm(10);
#endif

    hConsumer->stop(hTopic, hPartition);

    delete hConsumer;
    delete hTopic;

    RdKafka::wait_destroyed(5000);
}

void SimpleKafka1C::message(const variant_t &msg) {
    std::visit(overloaded{
            [&](const std::string &v) { AddError(ADDIN_E_INFO, extensionName(), v, false); },
            [&](const int32_t &v) {
                AddError(ADDIN_E_INFO, extensionName(), std::to_string(static_cast<int>(v)), false);
            },
            [&](const double &v) { AddError(ADDIN_E_INFO, extensionName(), std::to_string(v), false); },
            [&](const bool &v) {
                AddError(ADDIN_E_INFO, extensionName(), std::string(v ? u8"Истина" : u8"Ложь"), false);
            },
            [&](const std::tm &v) {
               std::ostringstream oss;
                oss.imbue(std::locale("ru_RU.utf8"));
                oss << std::put_time(&v, "%c");
                AddError(ADDIN_E_INFO, extensionName(), oss.str(), false);
            },
            [&](const std::vector<char> &v) {},
            [&](const std::monostate &) {}
    }, msg);
}

void SimpleKafka1C::sleep(const variant_t &delay) {
    using namespace std;

    this_thread::sleep_for(chrono::seconds(get<int32_t>(delay)));
}