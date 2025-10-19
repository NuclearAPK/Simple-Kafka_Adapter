#include <boost/algorithm/string/split.hpp>          // boost::algorithm::split
#include <boost/algorithm/string/classification.hpp> // boost::is_any_of
#include <boost/algorithm/string/replace.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/json.hpp>
#include <boost/json/monotonic_resource.hpp>
#ifdef _WINDOWS
#include <process.h>
#endif
#include <avro/Encoder.hh>
#include <avro/Specific.hh>
#include <avro/Compiler.hh>
#include <avro/Types.hh>
#include <avro/Generic.hh>
#include <avro/DataFile.hh>
#include <avro/Writer.hh>

#include <thread>
#include <chrono>

#include "md5.h"
#include "SimpleKafka1C.h"


//================================== Utilites ==========================================

char* slice(char* s, size_t from, size_t to)
{
	size_t j = 0;
	for (size_t i = from; i <= to; ++i)
		s[j++] = s[i];
	s[j] = 0;
	return s;
};

const std::string currentDateTime()
{
	std::chrono::time_point now = std::chrono::high_resolution_clock::now();
	tm current{};

#ifdef _WINDOWS
	time_t time = std::time(nullptr);
	localtime_s(&current, &time);
#else
	auto time = std::chrono::system_clock::to_time_t(now);
	current = *std::gmtime(&time);
#endif

	auto epoch = now.time_since_epoch();
	auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(epoch).count() % 1000000000;

	std::ostringstream oss{};
	oss << std::put_time(&current, "%Y-%m-%d %T.") << ns;
	return oss.str();
}

const std::string currentDateTime(char* format)
{
	std::chrono::time_point now = std::chrono::high_resolution_clock::now();
	tm current{};

#ifdef _WINDOWS
	time_t time = std::time(nullptr);
	localtime_s(&current, &time);
#else
	auto time = std::chrono::system_clock::to_time_t(now);
	current = *std::gmtime(&time);
#endif

	std::ostringstream oss{};
	oss << std::put_time(&current, format);
	return oss.str();
}

intmax_t getTimeStamp()
{
	time_t curtime = time(nullptr);
	intmax_t time = (intmax_t)curtime;
	return time;
}

//================================== Events callback ===================================

void SimpleKafka1C::clEventCb::event_cb(RdKafka::Event& event)
{
	std::ofstream eventFile{};
	std::ofstream statFile{};

	if (!logDir.empty())
	{

		std::string bufnameCS = consumerLogName;
		std::string bufnameST = statLogName;

		if (!clientid.empty())
		{
			bufnameCS = bufnameCS + clientid + "_";
			bufnameST = bufnameST + clientid + "_";
		}

		eventFile.open(logDir + bufnameCS + std::to_string(pid) + "_" + currentDateTime(formatLogFiles) + ".log", std::ios_base::app);
		
		if (statisticsOn) {
			statFile.open(logDir + bufnameST + std::to_string(pid) + "_" + currentDateTime(formatLogFiles) + ".log", std::ios_base::app);
		}		
	}

	switch (event.type())
	{
	case RdKafka::Event::EVENT_ERROR:
		if (event.fatal())
		{
			eventFile << currentDateTime() << "FATAL ";
		}
		eventFile << "ERROR (" << RdKafka::err2str(event.err())
			<< "): " << event.str() << std::endl;
		break;

	case RdKafka::Event::EVENT_STATS:
		if (statisticsOn) statFile << currentDateTime() << "\"STATS\": " << event.str() << std::endl;
		break;

	case RdKafka::Event::EVENT_LOG:
		char buf[512];
		sprintf(buf, " LOG-%i-%s: %s\n", event.severity(), event.fac().c_str(),
			event.str().c_str());
		eventFile << currentDateTime() << buf;
		break;

	case RdKafka::Event::EVENT_THROTTLE:
		eventFile << currentDateTime() << "THROTTLED: " << event.throttle_time() << "ms by "
			<< event.broker_name() << " id " << (int)event.broker_id()
			<< std::endl;
		break;

	default:
		eventFile << currentDateTime() << "EVENT: " << event.type() << " ("
			<< RdKafka::err2str(event.err()) << "): " << event.str()
			<< std::endl;
		break;
	}
}

void SimpleKafka1C::clDeliveryReportCb::dr_cb(RdKafka::Message& message)
{
	std::ofstream eventFile{};
	std::string status_name;

	delivered = message.status();

	if (!logDir.empty())
	{
		std::string bufname = producerLogName;

		if (!clientid.empty())
		{
			bufname = bufname + clientid + "_";
		}
		eventFile.open(logDir + bufname + std::to_string(pid) + "_" + currentDateTime(formatLogFiles) + ".log", std::ios_base::app);
	}

	if (eventFile.is_open())
	{
		switch (message.status())
		{
		case RdKafka::Message::MSG_STATUS_NOT_PERSISTED:
			status_name = "NotPersisted";
			break;
		case RdKafka::Message::MSG_STATUS_POSSIBLY_PERSISTED:
			status_name = "PossiblyPersisted";
			break;
		case RdKafka::Message::MSG_STATUS_PERSISTED:
			status_name = "Persisted";
			break;
		default:
			status_name = "Unknown?";
			break;
		}

		eventFile << currentDateTime();

		if (message.key()->length())
		{
			eventFile << " Key:" << *(message.key()) << ", ";
		}
		else
		{
			eventFile << " Hash:" << md5(static_cast<char*>(message.payload())) << ", ";
		}

		auto result = message.errstr();
		if (result.find(":") > 0) {
			boost::replace_all(result, ":", "");
		}

		eventFile << "Status:" << status_name << ", "
			<< "Details:" << result << ", "
			<< "Size:" << message.len() << ", "
			<< "Topic:" << message.topic_name() << ", "
			<< "Offset:" << message.offset() << ", "
			<< "Partition:" << message.partition() << ", "
			<< "BrokerID:" << message.broker_id() << std::endl;
	}
}

void SimpleKafka1C::clRebalanceCb::rebalance_cb(RdKafka::KafkaConsumer* consumer,
	RdKafka::ErrorCode err,
	std::vector<RdKafka::TopicPartition*>& partitions) 
	{
	if (err == RdKafka::ERR__ASSIGN_PARTITIONS) 
	{
		if (offsets.size()) 
		{

			for (unsigned int i = 0; i < partitions.size(); i++) 
			{
				for (unsigned int j = 0; j < offsets.size(); j++)
				{
					if (partitions[i]->topic() == offsets[j]->topic() &&
						partitions[i]->partition() == offsets[j]->partition()) {

						partitions[i]->set_offset(offsets[j]->offset());
						break;
					}
				}
			}

			consumer->assign(partitions);

			for (auto offset : offsets)
			{
				delete offset;
			}

			offsets.clear();
		}
	}
	else {
		consumer->unassign();
	}
}

//======================================================================================

std::string SimpleKafka1C::extensionName()
{
	return u8"SimpleKafka1C";
}

SimpleKafka1C::SimpleKafka1C()
{
	hProducer = nullptr;
	hConsumer = nullptr;

	clearMessageMetadata();

	logDirectory = std::make_shared<variant_t>(std::string(""));
	formatLogFiles = std::make_shared<variant_t>(std::string("%Y%m%d")); // format date in log files

	AddProperty(L"LogDirectory", L"КаталогЛогов", logDirectory);
	AddProperty(L"FormatLogFiles", L"ФорматИмениФайловЛога", formatLogFiles);
	AddProperty(L"Version", L"ВерсияКомпоненты", [&]()
		{
			auto s = std::string(Version);
			return std::make_shared<variant_t>(std::move(s)); });

    // The first method must be GetLastError
	AddMethod(L"GetLastError", L"ПолучитьСообщениеОбОшибке", this, &SimpleKafka1C::getLastError);
	AddMethod(L"SetParameter", L"УстановитьПараметр", this, &SimpleKafka1C::setParameter);
	AddMethod(L"InitializeProducer", L"ИнициализироватьПродюсера", this, &SimpleKafka1C::initProducer);
	AddMethod(L"Produce", L"ОтправитьСообщение", this, &SimpleKafka1C::produce,
		{ {2, -1}, {3, std::string("")}, {4, std::string("")} });
	AddMethod(L"ProduceWithWaitResult", L"ОтправитьСообщениеСОжиданиемРезультата", this, &SimpleKafka1C::produceWithWaitResult,
		{ {2, -1}, {3, std::string("")}, {4, std::string("")} });
	AddMethod(L"StopProducer", L"ОстановитьПродюсера", this, &SimpleKafka1C::stopProducer);

	AddMethod(L"InitializeConsumer", L"ИнициализироватьКонсьюмера", this, &SimpleKafka1C::initConsumer);
	AddMethod(L"Subscribe", L"Подписаться", this, &SimpleKafka1C::subscribe); // experemental
	AddMethod(L"Consume", L"Слушать", this, &SimpleKafka1C::consume);
	AddMethod(L"CommitOffset", L"ЗафиксироватьСмещение", this, &SimpleKafka1C::commitOffset, { {2, 0} });
	AddMethod(L"SetReadingPosition", L"УстановитьПозициюЧтения", this, &SimpleKafka1C::setReadingPosition, { {2, 0} });
	AddMethod(L"SetReadingPositions", L"УстановитьПозицииЧтения", this, &SimpleKafka1C::setReadingPositions); // experemental
	AddMethod(L"StopConsumer", L"ОстановитьКонсьюмера", this, &SimpleKafka1C::stopConsumer);
	AddMethod(L"SetWaitingTimeout", L"УстановитьТаймаутОжидания", this, &SimpleKafka1C::setWaitingTimeout);
	// + modern methods
	AddMethod(L"ReadMessage", L"ПрочитатьСообщение", this, &SimpleKafka1C::getMessage);
	AddMethod(L"GetMessageData", L"ПолучитьДанныеСообщения", this, &SimpleKafka1C::getMessageData, { {0, false} });
	AddMethod(L"GetMessageKey", L"ПолучитьКлючСообщения", this, &SimpleKafka1C::getMessageKey);
	AddMethod(L"GetMessageHeaders", L"ПолучитьЗаголовкиСообщения", this, &SimpleKafka1C::getMessageHeaders);
	AddMethod(L"GetMessageTopic", L"ПолучитьТопикСообщения", this, &SimpleKafka1C::getMessageTopicName);
	AddMethod(L"GetMessageBrokerID", L"ПолучитьИдентификаторБрокераСообщения", this, &SimpleKafka1C::getMessageBrokerID);
	AddMethod(L"GetMessageTimestamp", L"ПолучитьВременнуюМеткуСообщения", this, &SimpleKafka1C::getMessageTimestamp);
	AddMethod(L"GetMessagePartition", L"ПолучитьРазделСообщения", this, &SimpleKafka1C::getMessagePartition);
	AddMethod(L"GetMessageOffset", L"ПолучитьСмещениеСообщения", this, &SimpleKafka1C::getMessageOffset);
	// - modern methods

	// + admin api
	AddMethod(L"GetTopics", L"ПолучитьСписокТопиков", this, &SimpleKafka1C::getListOfTopics);
	AddMethod(L"getConsumerCurrentGroupOffset", L"ПолучитьСмещенияТекущегоКонсьюмера", this, &SimpleKafka1C::getConsumerCurrentGroupOffset, { {0, std::string("")}, {1, 5000}});
	AddMethod(L"getConsumerGroupOffsets", L"ПолучитьСмещенияГруппыКонсьюмеров", this, &SimpleKafka1C::getConsumerGroupOffsets, { {1, std::string("")}, {2, 5000}});
	AddMethod(L"CreateTopic", L"СоздатьТопик", this, &SimpleKafka1C::createTopic);
	// - admin api

	AddMethod(L"Sleep", L"Пауза", this, &SimpleKafka1C::sleep);
	AddMethod(L"SetLogDirectory", L"УстановитьКаталогЛогов", this, &SimpleKafka1C::setLogDirectory);
	AddMethod(L"SetFormatLogFiles", L"УстановитьФорматЛогов", this, &SimpleKafka1C::setFormatLogFiles);

	AddMethod(L"PutAvroSchema", L"СохранитьСхемуAVRO", this, &SimpleKafka1C::putAvroSchema);
	AddMethod(L"ConvertToAvroFormat", L"ПреобразоватьВФорматAVRO", this, &SimpleKafka1C::convertToAvroFormat);
	AddMethod(L"ProduceAvro", L"ОтправитьСообщениеAVRO", this, &SimpleKafka1C::produceAvro,
		{ {1, -1}, {3, std::string("")}, {4, std::string("")} });
	AddMethod(L"ProduceAvroWithWaitResult", L"ОтправитьСообщениеAVROСОжиданиемРезультата", this, &SimpleKafka1C::produceAvroWithWaitResult,
		{ {1, -1}, {3, std::string("")}, {4, std::string("")} });
	AddMethod(L"SaveAvroFile", L"СохранитьФайлAVRO", this, &SimpleKafka1C::saveAvroFile);

	waitMessageTimeout = 500;

#ifdef _WINDOWS
	pid = _getpid();
#else
	pid = getpid();
#endif
}

SimpleKafka1C::~SimpleKafka1C()
{
	clearMessageMetadata();

	stopConsumer();
	stopProducer();
}

//================================== Settings ==========================================

void SimpleKafka1C::setParameter(const variant_t& key, const variant_t& value)
{
	settings.push_back({ std::get<std::string>(key), std::get<std::string>(value) });
}

std::string SimpleKafka1C::clientID()
{
	std::string result;

	for (size_t i = 0; i < settings.size(); i++)
	{
		if (settings[i].Key == "client.id")
		{
			result = settings[i].Value;
			break;
		}
	}

	return result;
}

//================================== Producer ==========================================

bool SimpleKafka1C::initProducer(const variant_t& brokers)
{
	std::ofstream eventFile{};
	RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

	cl_dr_cb.logDir = std::get<std::string>(*logDirectory);
	cl_dr_cb.formatLogFiles = &*std::get<std::string>(*formatLogFiles).begin();
	cl_dr_cb.producerLogName = producerLogName;
	cl_dr_cb.pid = pid;
	cl_dr_cb.clientid = clientID();
	
	// events log - debug e.t.c...
	cl_event_cb.logDir = cl_dr_cb.logDir;
	cl_event_cb.formatLogFiles = cl_dr_cb.formatLogFiles;
	cl_event_cb.consumerLogName = producerLogName;
	cl_event_cb.statLogName = statLogName;
	cl_event_cb.pid = pid;
	cl_event_cb.clientid = cl_dr_cb.clientid;

	openEventFile(producerLogName, eventFile);
	if (eventFile.is_open()) eventFile << currentDateTime() << " Simple Kafka version: " << Version << " (librdkafka version: " << RdKafka::version_str() << ")" << std::endl;

	std::string tBrokers = std::get<std::string>(brokers);
	if (eventFile.is_open()) eventFile << currentDateTime() << " Info: initProducer. brokers-" << tBrokers << std::endl;

	for (size_t i = 0; i < settings.size(); i++)
	{
		if (conf->set(settings[i].Key, settings[i].Value, msg_err) != RdKafka::Conf::CONF_OK)
		{
			eventFile << currentDateTime() << " Error: " << msg_err << std::endl;
			return false;
		}

		if (settings[i].Key == "statistics.interval.ms") cl_event_cb.statisticsOn = true;
	}
	if (conf->set("metadata.broker.list", tBrokers, msg_err) != RdKafka::Conf::CONF_OK)
	{
		eventFile << currentDateTime() << " Error: " << msg_err << std::endl;
		return false;
	}

	conf->set("dr_cb", &cl_dr_cb, msg_err); // callback trigger
	conf->set("event_cb", &cl_event_cb, msg_err);

	hProducer = RdKafka::Producer::create(conf, msg_err);
	if (!hProducer)
	{
		eventFile << currentDateTime() << " Error: " << msg_err << std::endl;
		return false;
	}

	delete conf;

	return true;
}

int32_t SimpleKafka1C::produce(const variant_t& msg, const variant_t& topicName, const variant_t& partition, const variant_t& key, const variant_t& heads)
{
	cl_dr_cb.delivered = RdKafka::Message::MSG_STATUS_NOT_PERSISTED;
	if (hProducer == nullptr)
	{
		msg_err = u8"Продюсер не инициализирован";
		return -1;
	}

	std::string tTopicName = std::get<std::string>(topicName);
	auto currentPartition = std::get<int>(partition);

	std::ofstream eventFile{};
	openEventFile(producerLogName, eventFile);
	if (eventFile.is_open()) eventFile << currentDateTime() << " Info: produce. TopicName-" << tTopicName << " currentPartition-" << currentPartition << " avroFile.size()- " << avroFile.size() << std::endl;

	RdKafka::Headers* hdrs = nullptr;
	if (std::get<std::string>(heads).size() > 0)
	{
		std::vector<std::string> splitResult;
		boost::algorithm::split(splitResult, std::get<std::string>(heads), boost::is_any_of(";"));
		hdrs = RdKafka::Headers::create();
		for (std::string& s : splitResult)
		{
			std::vector<std::string> hKeyValue;
			boost::algorithm::split(hKeyValue, s, boost::is_any_of(","));
			if (hKeyValue.size() == 2)
				hdrs->add(hKeyValue[0], hKeyValue[1]);
		}
	}

retry:

	RdKafka::ErrorCode resp;

	if (std::holds_alternative<std::string>(msg))
	{
		resp = hProducer->produce(
			tTopicName,
			currentPartition == -1 ? RdKafka::Topic::PARTITION_UA : currentPartition,
			RdKafka::Producer::RK_MSG_COPY,
			const_cast<char*>(std::get<std::string>(msg).c_str()), std::get<std::string>(msg).size(),
			std::get<std::string>(key).c_str(), std::get<std::string>(key).size(),
			0,
			hdrs,
			nullptr);
	}
	else
	{
		auto d = std::get<std::vector<char>>(msg);
		unsigned char* pubMsg = new unsigned char[d.size()];

		for (size_t i = 0; i < d.size(); i++)
		{
			pubMsg[i] = d.at(i);
		}

		resp = hProducer->produce(
			tTopicName,
			currentPartition == -1 ? RdKafka::Topic::PARTITION_UA : currentPartition,
			RdKafka::Producer::RK_MSG_COPY,
			pubMsg, d.size(),
			std::get<std::string>(key).c_str(), std::get<std::string>(key).size(),
			0,
			hdrs,
			nullptr);
	}

	if (resp != RdKafka::ERR_NO_ERROR)
	{
		if (resp == RdKafka::ERR__QUEUE_FULL)
		{
			hProducer->poll(1000 /*block for max 1000ms*/);
			if (eventFile.is_open()) 
			{
				eventFile << currentDateTime() << " Error: " << "Достигнуто максимальное количество ожидающих сообщений: queue.buffering.max.message" << std::endl;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(1000));
			goto retry;
		}
		msg_err = RdKafka::err2str(resp);
		if (hdrs != nullptr)
		{
			delete hdrs;
		}
		cl_dr_cb.delivered = -1;
	}

	hProducer->poll(0);

	if (eventFile.is_open()) {
		if (!msg_err.empty())
			eventFile << currentDateTime() << " Error produce: " << msg_err << std::endl;
		else if (resp != RdKafka::ERR_NO_ERROR) 
			eventFile << " Errorcode produce: " << resp << std::endl;
		else
			eventFile << currentDateTime() << " Info: produce. Success" << std::endl;
	}

	return cl_dr_cb.delivered;
}

int32_t SimpleKafka1C::produceWithWaitResult(const variant_t& msg, const variant_t& topicName, const variant_t& partition, const variant_t& key, const variant_t& heads)
{
	if (produce(msg, topicName, partition, key, heads) != -1)
	{
		hProducer->flush(20 * 1000);		 // wait for max 20 seconds
		if (hProducer->outq_len() > 0) 
		{
			msg_err = u8"Не доставлено сообщений - " + hProducer->outq_len();

			std::ofstream eventFile{};
			openEventFile(producerLogName, eventFile);
			if (eventFile.is_open()) eventFile << currentDateTime() << " Info: produceWithWaitResult: " << msg_err << std::endl;

			return RdKafka::Message::MSG_STATUS_NOT_PERSISTED;
		}
		else if (cl_dr_cb.delivered != RdKafka::Message::MSG_STATUS_PERSISTED)
		{
			msg_err = u8"Не доставлено. Подробности см в логе";
		}
		return cl_dr_cb.delivered;
	}
	return -1;
}

int32_t SimpleKafka1C::produceAvro(const variant_t& topicName, const variant_t& partition, const variant_t& key, const variant_t& heads)
{
	cl_dr_cb.delivered = RdKafka::Message::MSG_STATUS_NOT_PERSISTED;
	if (hProducer == nullptr)
	{
		msg_err = u8"Продюсер не инициализирован";
		return -1;
	}
	if (avroFile.empty())
	{
		msg_err = u8"AVRO файл пустой";
		return -1;
	}

	std::string tTopicName = std::get<std::string>(topicName);
	auto currentPartition = std::get<int>(partition);
	std::ofstream eventFile{};

	openEventFile(producerLogName, eventFile);
	if (eventFile.is_open()) 
	{
		eventFile << currentDateTime() << " Info: produceAvro. TopicName-" << tTopicName << " currentPartition-" << currentPartition << " avroFile.size()- " << avroFile.size() << std::endl;
	}

retry:
	RdKafka::ErrorCode resp = hProducer->produce(
		tTopicName,
		currentPartition == -1 ? RdKafka::Topic::PARTITION_UA : currentPartition,
		RdKafka::Producer::RK_MSG_COPY,
		avroFile.data(),
		avroFile.size(),
		std::get<std::string>(key).c_str(), std::get<std::string>(key).size(),
		0,
		nullptr,
		nullptr);

	if (resp != RdKafka::ERR_NO_ERROR)
	{
		if (resp == RdKafka::ERR__QUEUE_FULL)
		{
			hProducer->poll(1000 /*block for max 1000ms*/);
			if (eventFile.is_open()) 
			{
				eventFile << currentDateTime() << " Error: " << "Достигнуто максимальное количество ожидающих сообщений: queue.buffering.max.message" << std::endl;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(1000));
			goto retry;
		}
		msg_err = RdKafka::err2str(resp);
		cl_dr_cb.delivered = -1;
	}

	hProducer->poll(0);

	if (eventFile.is_open()) 
	{
		if (!msg_err.empty())
			eventFile << currentDateTime() << " Error produceAvro: " << msg_err << std::endl;
		else if (resp != RdKafka::ERR_NO_ERROR) 
			eventFile << " Errorcode produceAvro: " << resp << std::endl;
		else
			eventFile << currentDateTime() << " Info produceAvro. Success" << std::endl;
	}

	return cl_dr_cb.delivered;
}

int32_t SimpleKafka1C::produceAvroWithWaitResult(const variant_t& topicName, const variant_t& partition, const variant_t& key, const variant_t& heads)
{
	if (produceAvro(topicName, partition, key, heads) != -1)
	{
		hProducer->flush(20 * 1000);		 // wait for max 10 seconds
		if (hProducer->outq_len() > 0)
		{
			msg_err = u8"Не доставлено сообщений - " + hProducer->outq_len();

			std::ofstream eventFile{};
			openEventFile(producerLogName, eventFile);
			if (eventFile.is_open()) eventFile << currentDateTime() << " Info: produceAvroWithWaitResult: " << msg_err << std::endl;

			return RdKafka::Message::MSG_STATUS_NOT_PERSISTED;
		}
		else if (cl_dr_cb.delivered != RdKafka::Message::MSG_STATUS_PERSISTED)
		{
			msg_err = u8"Не доставлено. Подробности см в логе";
		}
		return cl_dr_cb.delivered;
	}
    return -1;
}

bool SimpleKafka1C::stopProducer()
{
	if (hProducer != nullptr)
	{
		hProducer->flush(10 * 1000 /* wait for max 10 seconds */);
		delete hProducer;
		hProducer = nullptr;
	}
	return true;
}

//================================== Consumer ==========================================

bool SimpleKafka1C::initConsumer(const variant_t& brokers)
{
	std::ofstream eventFile{};
	RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

	cl_event_cb.logDir = std::get<std::string>(*logDirectory);
	cl_event_cb.formatLogFiles = &*std::get<std::string>(*formatLogFiles).begin();
	cl_event_cb.consumerLogName = consumerLogName;
	cl_event_cb.statLogName = statLogName;
	cl_event_cb.pid = pid;
	cl_event_cb.clientid = clientID();

	openEventFile(consumerLogName, eventFile);
	if (eventFile.is_open()) eventFile << currentDateTime() << " Simple Kafka version: " << Version << " (librdkafka version: " << RdKafka::version_str() << ")" << std::endl;
	
	// дополнительные параметры
	for (size_t i = 0; i < settings.size(); i++)
	{
		if (conf->set(settings[i].Key, settings[i].Value, msg_err) != RdKafka::Conf::CONF_OK)
		{
			if (eventFile.is_open()) eventFile << currentDateTime() << msg_err << std::endl;
			return false;
		}

		if (settings[i].Key == "statistics.interval.ms") cl_event_cb.statisticsOn = true;
	}
	// обязательный параметр
	if (conf->set("metadata.broker.list", std::get<std::string>(brokers), msg_err) != RdKafka::Conf::CONF_OK)
	{
		if (eventFile.is_open()) eventFile << currentDateTime() << msg_err << std::endl;
		return false;
	}

	// обратные вызовы для получения статистики и получения ошибок для дальнейшей обработки
	conf->set("event_cb", &cl_event_cb, msg_err);

	if (cl_rebalance_cb.offsets.size() > 0)
	{
		conf->set("rebalance_cb", &cl_rebalance_cb, msg_err);
	}

	if (eventFile.is_open()) eventFile << currentDateTime() << " Info: initConsumer. brokers-" << std::get<std::string>(brokers) << std::endl;

	hConsumer = RdKafka::KafkaConsumer::create(conf, msg_err);
	if (!hConsumer)
	{
		if (eventFile.is_open()) eventFile << currentDateTime() << msg_err << std::endl;
		return false;
	}

	if (eventFile.is_open())
		eventFile << currentDateTime() << " Created consumer: " << hConsumer->name() << std::endl;

	delete conf;
	return true;
}

bool SimpleKafka1C::subscribe(const variant_t& topic)
{
	std::ofstream eventFile{};
	openEventFile(consumerLogName, eventFile);
	if (hConsumer == nullptr)
	{
		msg_err = u8"Консьюмер не инициализирован";
		return false;
	}

	std::vector<std::string> topics;
	boost::algorithm::split(topics, std::get<std::string>(topic), boost::is_any_of(","));

	RdKafka::ErrorCode resp = hConsumer->subscribe(topics);

	if (resp != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(resp);
		if (eventFile.is_open()) eventFile << currentDateTime() << " Failed to start subscribe consumer: " << msg_err << std::endl;
		return false;
	}

	return true;
}

bool SimpleKafka1C::setWaitingTimeout(const variant_t& timeout)
{
	waitMessageTimeout = std::get<int32_t>(timeout);
	return true;
}

bool SimpleKafka1C::setReadingPosition(const variant_t& topicName, const variant_t& offset, const variant_t& partition)
{
	auto assignOffset = std::get<int32_t>(offset);
	auto assignTopic = std::get<std::string>(topicName);
	auto assignPartition = std::get<int32_t>(partition);

	RdKafka::TopicPartition* ptr = RdKafka::TopicPartition::create(assignTopic, assignPartition, assignOffset);
	cl_rebalance_cb.offsets.push_back(ptr);
	return true;
}

bool SimpleKafka1C::setReadingPositions(const variant_t& jsonTopicPartitions)
{
	using namespace boost::json;
	std::string jsonString = std::get<std::string>(jsonTopicPartitions);

	auto parsed_data = parse(jsonString);
	auto meta = parsed_data.at("metadata");

	if (meta.is_array()) 
	{
		for (size_t i = 0; i < meta.as_array().size(); i++)
		{
			std::string topic_ = value_to<std::string>(meta.at(i).at("topic"));
			int partition_ = value_to<int>(meta.at(i).at("partition"));
			long long offset_ = value_to<long long>(meta.at(i).at("offset"));

			RdKafka::TopicPartition* ptr = RdKafka::TopicPartition::create(topic_, partition_, offset_);
			cl_rebalance_cb.offsets.push_back(ptr);
		}
	}
	return true;
}

std::string SimpleKafka1C::consume()
{
	if (hConsumer == nullptr)
	{
		msg_err = u8"Консьюмер не инициализирован";
		return EMPTYSTR;
	}

	std::stringstream s{};
	msg_err = EMPTYSTR;
	std::ofstream eventFile{};
	boost::property_tree::ptree jsonObj;
	RdKafka::Headers* headers;
	RdKafka::Message* msg = hConsumer->consume(waitMessageTimeout);
	RdKafka::ErrorCode resultConsume = msg->err();

	openEventFile(consumerLogName, eventFile);
	if (resultConsume == RdKafka::ERR_NO_ERROR)
	{
		boost::property_tree::ptree headersChildren;
		auto payload = static_cast<char*>(msg->payload());

		if (msg->key() && (*msg->key()).length() > 0)
		{
			jsonObj.put("key", *msg->key());
		}

		headers = msg->headers();
		if (headers)
		{
			std::vector<RdKafka::Headers::Header> hdrs = headers->get_all();
			for (size_t i = 0; i < hdrs.size(); i++) 
			{
				RdKafka::Headers::Header hdr = hdrs[i];

				boost::property_tree::ptree node;
				node.put("key", hdr.key().c_str());
				node.put("value", (const char*)hdr.value());

				headersChildren.push_back(boost::property_tree::ptree::value_type("", node));
			}
		}

		RdKafka::MessageTimestamp ts = msg->timestamp();

		jsonObj.put("partition", msg->partition());
		jsonObj.put("offset", (long)msg->offset());
		jsonObj.put("message", std::string(slice(payload, 0, msg->len())));
		jsonObj.put("topic", msg->topic_name());
		jsonObj.put("broker_id", msg->broker_id());
		jsonObj.put("timestamp", ts.timestamp);

		if (headersChildren.size())
		{
			jsonObj.put_child("headers", headersChildren);
		}

		delete msg;
	}
	else
	{
		if (resultConsume != RdKafka::ERR__TIMED_OUT) {
			msg_err = msg->errstr();
			if (eventFile.is_open())
			{
				eventFile << currentDateTime() << " Error: " << msg_err << std::endl;
			}
		}
		delete msg;
		return EMPTYSTR;
	}
	boost::property_tree::write_json(s, jsonObj, true);

	return s.str();
}

bool SimpleKafka1C::getMessage()
{
	if (hConsumer == nullptr)
	{
		msg_err = u8"Консьюмер не инициализирован";
		return false;
	}

	RdKafka::Message* msg = hConsumer->consume(waitMessageTimeout);
	RdKafka::ErrorCode resultConsume = msg->err();

	std::ofstream eventFile;

	openEventFile(consumerLogName, eventFile);
	clearMessageMetadata();
	if (resultConsume == RdKafka::ERR_NO_ERROR)
	{
		this->messageLen = msg->len();

		u_char* charBuf = (u_char*)msg->payload();
		std::vector<char> binaryData(charBuf, charBuf + this->messageLen);
		messageData = binaryData;

		if (msg->key() && (*msg->key()).length() > 0)
		{
			this->key = *msg->key();
		}

		RdKafka::Headers* headers;
		headers = msg->headers();
		if (headers)
		{
			std::vector<RdKafka::Headers::Header> hdrs = headers->get_all();
			for (size_t i = 0; i < hdrs.size(); i++) {
				RdKafka::Headers::Header hdr = hdrs[i];

				HeadersMessage mh{ hdr.key().c_str(), (const char*)hdr.value() };
				messageHeaders.push_back(mh);
			}
		}

		RdKafka::MessageTimestamp ts = msg->timestamp();

		this->timestamp = ts.timestamp;
		this->partition = msg->partition();
		this->offset = msg->offset();
		this->topic = msg->topic_name();
		this->broker_id = msg->broker_id();

		delete msg;
	}
	else
	{
		if (resultConsume != RdKafka::ERR__TIMED_OUT) {
			msg_err = msg->errstr();
			if (eventFile.is_open()) eventFile << currentDateTime() << " Error: " << msg_err << std::endl;
		}
		delete msg;
		return false;
	}

	return true;
}

variant_t SimpleKafka1C::getMessageData(const variant_t& binaryResult)
{
	bool res = std::get<bool>(binaryResult);
	if (res)
	{
		return this->messageData;
	}
	else
	{
		return std::string(messageData.begin(), messageData.end());
	}
}

std::string SimpleKafka1C::getMessageKey()
{
	return this->key;
}

std::string SimpleKafka1C::getMessageHeaders()
{
	std::stringstream s{};
	boost::property_tree::ptree jsonObj;
	boost::property_tree::ptree headersChildren;

	for (size_t i = 0; i < messageHeaders.size(); i++)
	{
		boost::property_tree::ptree node;
		node.put("key", messageHeaders[i].Key);
		node.put("value", messageHeaders[i].Value);

		headersChildren.push_back(boost::property_tree::ptree::value_type("", node));
	}

	if (headersChildren.size())
	{
		jsonObj.put_child("headers", headersChildren);
	}

	boost::property_tree::write_json(s, jsonObj, true);
	return s.str();
}

int32_t SimpleKafka1C::getMessageOffset()
{
	return (int32_t) this->offset;
}

std::string SimpleKafka1C::getMessageTopicName()
{
	return this->topic;
}

int32_t SimpleKafka1C::getMessageBrokerID()
{
	return this->broker_id;
}

float SimpleKafka1C::getMessageTimestamp()
{
	return (int64_t)this->timestamp/1000;
}

int32_t SimpleKafka1C::getMessagePartition()
{
	return this->partition;
}

bool SimpleKafka1C::commitOffset(const variant_t& topicName, const variant_t& offset, const variant_t& partition)
{
	std::vector<RdKafka::TopicPartition*> offsets;
	std::int32_t tOffset = std::get<std::int32_t>(offset);
	std::int32_t tPartition = std::get<std::int32_t>(partition);
	std::string tTopicName = std::get<std::string>(topicName);

	RdKafka::TopicPartition* ptr = RdKafka::TopicPartition::create(tTopicName, tPartition, tOffset);
	offsets.push_back(ptr);

	if (hConsumer->commitSync(offsets) != RdKafka::ERR_NO_ERROR)
	{
		return false;
	}
	return true;
}

bool SimpleKafka1C::stopConsumer()
{
	if (hConsumer != nullptr)
	{
		hConsumer->close();
		delete hConsumer;
		RdKafka::wait_destroyed(10 * 1000);
		hConsumer = nullptr;
	}
	return true;
}

void SimpleKafka1C::clearMessageMetadata()
{
	this->messageData.clear();
	this->messageHeaders.clear();

	this->messageLen = 0;
	this->timestamp = 0;
	this->key = EMPTYSTR;
	this->topic = EMPTYSTR;
	this->broker_id = 0;
	this->partition = 0;
	this->offset = 0;
}

//================================== Admin API =========================================

std::string SimpleKafka1C::getListOfTopics(const variant_t& brokers)
{
	std::string result;
	std::stringstream s{};
	std::ofstream eventFile{};

	boost::property_tree::ptree jsonObj;
	RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

	std::string tBrokers = std::get<std::string>(brokers);
	
	// дополнительные параметры
	for (size_t i = 0; i < settings.size(); i++)
	{
		if (conf->set(settings[i].Key, settings[i].Value, msg_err) != RdKafka::Conf::CONF_OK)
		{
			if (eventFile.is_open()) eventFile << currentDateTime() << msg_err << std::endl;
			return result;
		}
	}	

	if (conf->set("metadata.broker.list", tBrokers, msg_err) != RdKafka::Conf::CONF_OK)
	{
		return result;
	}

	// создаем фейкового продюсера
	RdKafka::Producer* producer = RdKafka::Producer::create(conf, msg_err);
	if (!producer)
	{
		msg_err = u8"Ошибка создания фейкового продюсера";
		return result;
	}

	RdKafka::Topic* topicKafka = nullptr;
	class RdKafka::Metadata* metadata;
	RdKafka::ErrorCode err = producer->metadata(true, topicKafka, &metadata, 5000);
	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(err);
		return result;
	}

	RdKafka::Metadata::TopicMetadataIterator it;
	boost::property_tree::ptree topicsChildren;

	for (it = metadata->topics()->begin(); it != metadata->topics()->end(); ++it)
	{
		boost::property_tree::ptree node;
		node.put("topic", (*it)->topic().c_str());
		node.put("partitions", (*it)->partitions()->size());

		topicsChildren.push_back(boost::property_tree::ptree::value_type("", node));
	}

	delete metadata;
	delete conf;

	if (topicsChildren.size())
	{
		jsonObj.put_child("topics", topicsChildren);
	}
	boost::property_tree::write_json(s, jsonObj, true);

	return s.str();
}

//extern "C"
//{
//	int _createTopic(char& broker, char& topicname, int partition_cnt, int rep_factor);
//}
//
//int _createTopic(const char* broker, const char* topicname, int partition_cnt, int rep_factor)
//{
//	char errstr[512];
//	int timeout_ms = 10000;
//	const size_t newt_cnt = 1;
//	size_t res_cnt;
//
//	rd_kafka_t* rk;
//	rd_kafka_NewTopic_t* newt[1];
//	rd_kafka_queue_t* rkqu;
//	rd_kafka_event_t* rkev;
//	rd_kafka_AdminOptions_t* options;
//	rd_kafka_resp_err_t err;
//
//	const rd_kafka_CreateTopics_result_t* res;
//	const rd_kafka_topic_result_t** terr;
//
//	rd_kafka_conf_t* conf; /* Temporary configuration object */
//
//	rkqu = rd_kafka_queue_new(rk);
//
//	// config
//	conf = rd_kafka_conf_new();
//
//	if (rd_kafka_conf_set(conf, "bootstrap.servers", broker, errstr,
//		sizeof(errstr)) != RD_KAFKA_CONF_OK) {
//		fprintf(stderr, "%s\n", errstr);
//		return false;
//	}
//
//	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
//	if (!rk) {
//		fprintf(stderr, "%% Failed to create new producer: %s\n",
//			errstr);
//		return false;
//	}
//
//	newt[0] = rd_kafka_NewTopic_new(topicname, partition_cnt, rep_factor,
//		errstr, sizeof(errstr));
//
//	//for (size_t i = 0; i < settings.size(); i++)
//	//	rd_kafka_NewTopic_set_config(newt[0], settings[i].Key.c_str(), settings[i].Value.c_str());
//
//	options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_CREATETOPICS);
//	err = rd_kafka_AdminOptions_set_operation_timeout(options, timeout_ms, errstr, sizeof(errstr));
//
//	rd_kafka_CreateTopics(rk, newt, newt_cnt, options, rkqu);
//	rkev = rd_kafka_queue_poll(rkqu, timeout_ms + 2000);
//	res = rd_kafka_event_CreateTopics_result(rkev);
//	terr = rd_kafka_CreateTopics_result_topics(res, &res_cnt);
//
//	rd_kafka_event_destroy(rkev);
//	rd_kafka_queue_destroy(rkqu);
//	rd_kafka_AdminOptions_destroy(options);
//	rd_kafka_NewTopic_destroy(newt[0]);
//	rd_kafka_destroy(rk);
//
//	if (res_cnt == newt_cnt) {
//		return 1;
//	}
//	else {
//		return 0;
//	}
//}

// TODO: refactoring
bool SimpleKafka1C::createTopic(const variant_t& brokers, const variant_t& topicName, const variant_t& partition, const variant_t& replication_factor)
{
	//int result;

	//auto topicname = std::get<std::string>(topicName).c_str();
	//auto partition_cnt = (int)std::get<std::int32_t>(partition);
	//auto rep_factor = (int)std::get<std::int32_t>(replication_factor);
	//auto broker = std::get<std::string>(brokers).c_str();

	//result = _createTopic(broker, topicname, partition_cnt, rep_factor);

	//if (result == 0) {
	//	return true;
	//}
	//else {
	//	return false;
	//}
	return true;
}

std::string SimpleKafka1C::getConsumerCurrentGroupOffset(const variant_t& times, const variant_t& timeout)
{
	if (hConsumer == nullptr)
	{
		msg_err = u8"Консьюмер не инициализирован";
		return EMPTYSTR;
	}

	std::stringstream s{};
	long long timeline = 0;
	std::string str_timeline = std::get<std::string>(times);
	
	if (!str_timeline.empty()) 
	{
		timeline = std::stoll(str_timeline);
	}
	
	auto tm = std::get<int32_t>(timeout);

	boost::property_tree::ptree jsonObj;
	boost::property_tree::ptree topicsChildren;

	class RdKafka::Metadata* metadata;
	hConsumer->metadata(true, nullptr, &metadata, 1000);

	std::vector<RdKafka::TopicPartition*> partitions;
	RdKafka::Metadata::TopicMetadataIterator it;
	typedef RdKafka::TopicMetadata::PartitionMetadataIterator PartitionIterator;

	for (it = metadata->topics()->begin(); it != metadata->topics()->end(); ++it)
	{
		for (PartitionIterator partIt = (*it)->partitions()->begin(); partIt != (*it)->partitions()->end(); ++partIt)
		{
			if (timeline > 0) {
				RdKafka::TopicPartition* tp = RdKafka::TopicPartition::create((*it)->topic().c_str(), (*partIt)->id(), timeline);
				partitions.push_back(tp);
			}
			else
			{
				RdKafka::TopicPartition* tp = RdKafka::TopicPartition::create((*it)->topic().c_str(), (*partIt)->id());
				partitions.push_back(tp);
			}
		}
	}

	RdKafka::ErrorCode err;

	if (timeline == 0) {
		do {
			err = hConsumer->committed(partitions, tm);
			tm = tm + 1000;
		} while (err);
	}
	else 
	{
		do {
			err = hConsumer->offsetsForTimes(partitions, tm);
			tm = tm + 1000;
		} while (err);
	}

	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(err);
		return EMPTYSTR;
	}

	delete metadata;

	for (auto tp : partitions)
	{
		boost::property_tree::ptree node;
		node.put("topic", tp->topic());
		node.put("partition", tp->partition());
		node.put("offset", tp->offset());

		topicsChildren.push_back(boost::property_tree::ptree::value_type("", node));
		delete tp;
	}

	if (topicsChildren.size())
	{
		jsonObj.put_child("metadata", topicsChildren);
	}
	boost::property_tree::write_json(s, jsonObj, true);

	return s.str();
}

std::string SimpleKafka1C::getConsumerGroupOffsets(const variant_t& brokers, const variant_t& times, const variant_t& timeout)
{
	initConsumer(brokers);
	std::string result = getConsumerCurrentGroupOffset(times, timeout);
	stopConsumer();

	return result;
}

std::string SimpleKafka1C::getTopicOptions(const variant_t& topicName)
{
	std::string result;
	std::stringstream s{};

	boost::property_tree::ptree jsonObj;


	
	//RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	//RdKafka::Conf* tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

	//tconf->dump();

	//boost::property_tree::write_json(s, jsonObj, true);
	return s.str();
}

//================================== Utilites ==========================================

bool SimpleKafka1C::sleep(const variant_t& delay)
{
	using namespace std;
	this_thread::sleep_for(chrono::seconds(get<int32_t>(delay)));
	return true;
}

bool SimpleKafka1C::setLogDirectory(const variant_t& logDir)
{
	std::string ldir = std::get<std::string>(logDir);
	logDirectory = std::make_shared<variant_t>(ldir);
	return true;
}

bool SimpleKafka1C::setFormatLogFiles(const variant_t& format)
{
	std::string lformat = std::get<std::string>(format);
	formatLogFiles = std::make_shared<variant_t>(lformat);
	return true;
}

void SimpleKafka1C::openEventFile(const std::string& logName, std::ofstream& eventFile)
{
	if (!cl_event_cb.logDir.empty())
	{
		std::string bufname = logName;
		if (!cl_event_cb.clientid.empty())
		{
			bufname = bufname + cl_event_cb.clientid + "_";
		}
		eventFile.open(cl_event_cb.logDir + bufname + std::to_string(pid) + "_" + currentDateTime(cl_event_cb.formatLogFiles) + ".log", std::ios_base::app);
	}
}


//================================== Avro ==========================================

bool SimpleKafka1C::putAvroSchema(const variant_t& schemaJsonName, const variant_t& schemaJson)
{
	try
	{
		// Проверяем, существует ли схема с таким именем
		auto it = schemesMap.find(std::get<std::string>(schemaJsonName));

		if (it == schemesMap.end())
		{
			// Схема не существует, компилируем и добавляем ее в map
			const avro::ValidSchema compiledScheme = avro::compileJsonSchemaFromString(std::get<std::string>(schemaJson));
			schemesMap[std::get<std::string>(schemaJsonName)] = compiledScheme;
		}
	}
	catch (std::exception const& ex)
	{
		msg_err = "Scheme compileError '";
		msg_err += ex.what();
	}

	return msg_err.empty();
}

bool SimpleKafka1C::convertToAvroFormat(const variant_t& msgJson, const variant_t& schemaJsonName)
{
	avroFile.clear();
	std::string key;
	std::string type;
	auto it = schemesMap.find(std::get<std::string>(schemaJsonName));
	avro::ValidSchema schema;

	if (it != schemesMap.end())
	{
		schema = it->second;
	}
	else
	{
		msg_err = u8"Имя схемы не известно - " + std::get<std::string>(schemaJsonName);
		return false;
	}

	avro::GenericDatum datum(schema);
	if (datum.type() != avro::AVRO_RECORD)
	{
		msg_err = u8"Некорректная схема";
		return false;
	}

	// Разбираем исходный json
	// Данные приходят в формате {"id": ["id_1", "id_1", "id_1", ...], "rmis_id": ["rmis_id_1", "rmis_id_2", "rmis_id_3", ...], ... }
	// Для корректной записи в Avro требуется данные преобразовать в формат: [{"id: "id_1", "rmis_id": "rmis_id_1", ...}, {"id: "id_2", "rmis_id": "rmis_id_2", ...}, {"id: "id_3", "rmis_id": "rmis_id_3", ...}, ...]

	boost::json::monotonic_resource mr;
	boost::json::value jsonInput_t;
	try
	{
		jsonInput_t = boost::json::parse(std::get<std::string>(msgJson), &mr);
	}
	catch (std::exception const& ex)
	{
		msg_err = "Error parsing scheme - ";
		msg_err += ex.what();
		return false;
	}
	const boost::json::object jsonInput = jsonInput_t.as_object();

	MemoryOutputStream* memOutStr = new MemoryOutputStream(100000);		// объект будет удален через unique_ptr при закрытии DataFileWriter
	std::unique_ptr<avro::OutputStream> os(memOutStr);
   	avro::DataFileWriter<avro::GenericDatum> writer(std::move(os), schema);

	try
	{
		// Получаем количество элементов в поле (в каждом поле должен быть массив с одинаковым количеством элементов)
		const auto first_array = jsonInput.cbegin();
		const size_t numElements = first_array->value().as_array().size();
		std::string err;
		for (size_t i = 0; i < numElements; i++)
		{
			// построчное преобразование
			boost::json::object jsonRecord;
			for (auto it = jsonInput.cbegin(); it != jsonInput.cend(); ++it)
			{
				const std::string& field_name = it->key_c_str();
				const boost::json::value& field_data = it->value();
				jsonRecord[field_name] = field_data.as_array().at(i);
			}

			avro::GenericRecord& record = datum.value<avro::GenericRecord>();
			for (auto field = jsonRecord.cbegin(); field != jsonRecord.cend(); ++field)
			{
				avro::GenericDatum& fieldDatum = record.field(field->key_c_str());
				type = toString(fieldDatum.type());
				key = field->key_c_str();

				// Если это объединение типов, например, type: ["null", "long"], то тогда по умолчанию устанавливаем второй тип, а затем проверяем значения
				// Тип устанавливается при помощи функции selectBranch() 
				if (fieldDatum.isUnion())
				{
					fieldDatum.selectBranch(1);
					switch (fieldDatum.type())
					{
					case avro::AVRO_STRING:
						if (field->value().is_null())
							fieldDatum.selectBranch(0);
						else
							fieldDatum.value<std::string>() = field->value().as_string();
						break;
					case avro::AVRO_LONG:
						if (field->value().is_null())
							fieldDatum.selectBranch(0);
						else
							fieldDatum.value<int64_t>() = field->value().as_int64();
						break;
					case avro::AVRO_INT:
						if (field->value().is_null())
							fieldDatum.selectBranch(0);
						else
							fieldDatum.value<int32_t>() = (int32_t)field->value().as_int64();
						break;
					case avro::AVRO_FLOAT:
						if (field->value().is_null())
							fieldDatum.selectBranch(0);
						else 
						fieldDatum.value<float>() = (float)field->value().as_double();
						break;
					case avro::AVRO_DOUBLE:
						if (field->value().is_null())
							fieldDatum.selectBranch(0);
						else 
							fieldDatum.value<double>() = field->value().as_double();
						break;
					case avro::AVRO_BOOL:
						if (field->value().is_null())
							fieldDatum.selectBranch(0);
						else 
							fieldDatum.value<bool>() = field->value().as_bool();
						break;
					case avro::AVRO_NULL:
						fieldDatum.value<avro::null>() = avro::null();
						break;
					case avro::AVRO_UNION:
						break;
					default:
						msg_err += u8"Unsupported type '" + type + u8"' on '" + key + u8"'. Supported: AVRO_STRING, AVRO_LONG, AVRO_INT, AVRO_FLOAT, AVRO_DOUBLE, AVRO_BOOL, AVRO_NULL, AVRO_UNION. ";
						break;
					}
				}
				else
				{
					switch (fieldDatum.type())
					{
					case avro::AVRO_STRING:
						fieldDatum.value<std::string>() = field->value().as_string();
						break;
					case avro::AVRO_LONG:
						fieldDatum.value<int64_t>() = field->value().as_int64();
						break;
					case avro::AVRO_INT:
						fieldDatum.value<int32_t>() = (int32_t)field->value().as_int64();
						break;
					case avro::AVRO_FLOAT:
						fieldDatum.value<float>() = (float)field->value().as_double();
						break;
					case avro::AVRO_DOUBLE:
						fieldDatum.value<double>() = field->value().as_double();
						break;
					case avro::AVRO_BOOL:
						fieldDatum.value<bool>() = field->value().as_bool();
						break;
					case avro::AVRO_NULL:
						fieldDatum.value<avro::null>() = avro::null();
						break;
					case avro::AVRO_UNION:
						break;
					default:
						msg_err += "Unsupported type '" + type + "' on '" + key + "'. Supported: AVRO_STRING, AVRO_LONG, AVRO_INT, AVRO_FLOAT, AVRO_DOUBLE, AVRO_BOOL, AVRO_NULL, AVRO_UNION. ";
						break;
					}
				}
			}
			writer.write(datum);
		}

		writer.flush();
		memOutStr->snapshot(avroFile);
	}
	catch (std::exception const& ex)
	{
		msg_err += "Error while proceesing key '" + key + "' with type '" + type + "' - " + ex.what();
	}
	writer.close();

	return msg_err.empty();
}

bool SimpleKafka1C::saveAvroFile(const variant_t& fileName)
{
	if (avroFile.empty())
	{
		msg_err = u8"AVRO файл пустой";
		return false;
	}

	try
	{
		std::ofstream out(std::get<std::string>(fileName), std::ios::out | std::ios::binary);
		out.write(reinterpret_cast<const char*>(avroFile.data()), avroFile.size());
		out.close();
	}
	catch (std::exception const& ex)
	{
		msg_err = ex.what();
	}

	return msg_err.empty();
}
