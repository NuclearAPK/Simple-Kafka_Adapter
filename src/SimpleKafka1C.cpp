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

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <thread>
#include <chrono>

#include "md5.h"
#include "SimpleKafka1C.h"

// Undefine Windows API macros that conflict with protobuf
#ifdef _WINDOWS
#undef GetMessage
#endif


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

//================================== Protobuf helpers ===================================

// Helper to convert JSON value to protobuf field
bool SetProtobufFieldFromJson(google::protobuf::Message* message,
	const google::protobuf::FieldDescriptor* field,
	const boost::json::value& json_value)
{
	const google::protobuf::Reflection* reflection = message->GetReflection();

	try {
		switch (field->type()) {
		case google::protobuf::FieldDescriptor::TYPE_STRING:
			if (json_value.is_string())
				reflection->SetString(message, field, std::string(json_value.as_string()));
			break;
		case google::protobuf::FieldDescriptor::TYPE_INT32:
		case google::protobuf::FieldDescriptor::TYPE_SINT32:
		case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
			if (json_value.is_int64())
				reflection->SetInt32(message, field, static_cast<int32_t>(json_value.as_int64()));
			break;
		case google::protobuf::FieldDescriptor::TYPE_INT64:
		case google::protobuf::FieldDescriptor::TYPE_SINT64:
		case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
			if (json_value.is_int64())
				reflection->SetInt64(message, field, json_value.as_int64());
			break;
		case google::protobuf::FieldDescriptor::TYPE_UINT32:
		case google::protobuf::FieldDescriptor::TYPE_FIXED32:
			if (json_value.is_uint64())
				reflection->SetUInt32(message, field, static_cast<uint32_t>(json_value.as_uint64()));
			break;
		case google::protobuf::FieldDescriptor::TYPE_UINT64:
		case google::protobuf::FieldDescriptor::TYPE_FIXED64:
			if (json_value.is_uint64())
				reflection->SetUInt64(message, field, json_value.as_uint64());
			break;
		case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
			if (json_value.is_double())
				reflection->SetDouble(message, field, json_value.as_double());
			break;
		case google::protobuf::FieldDescriptor::TYPE_FLOAT:
			if (json_value.is_double())
				reflection->SetFloat(message, field, static_cast<float>(json_value.as_double()));
			break;
		case google::protobuf::FieldDescriptor::TYPE_BOOL:
			if (json_value.is_bool())
				reflection->SetBool(message, field, json_value.as_bool());
			break;
		case google::protobuf::FieldDescriptor::TYPE_MESSAGE:
		{
			if (json_value.is_object()) {
				google::protobuf::Message* sub_message = reflection->MutableMessage(message, field);
				const boost::json::object& obj = json_value.as_object();
				const google::protobuf::Descriptor* sub_descriptor = sub_message->GetDescriptor();
				for (auto& kv : obj) {
					const google::protobuf::FieldDescriptor* sub_field =
						sub_descriptor->FindFieldByName(std::string(kv.key()));
					if (sub_field) {
						SetProtobufFieldFromJson(sub_message, sub_field, kv.value());
					}
				}
			}
			break;
		}
		default:
			return false;
		}
		return true;
	}
	catch (...) {
		return false;
	}
}

// Helper to convert protobuf field to JSON value
boost::json::value GetJsonFromProtobufField(const google::protobuf::Message& message,
	const google::protobuf::FieldDescriptor* field)
{
	const google::protobuf::Reflection* reflection = message.GetReflection();

	switch (field->type()) {
	case google::protobuf::FieldDescriptor::TYPE_STRING:
		return boost::json::string(reflection->GetString(message, field));
	case google::protobuf::FieldDescriptor::TYPE_INT32:
	case google::protobuf::FieldDescriptor::TYPE_SINT32:
	case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
		return reflection->GetInt32(message, field);
	case google::protobuf::FieldDescriptor::TYPE_INT64:
	case google::protobuf::FieldDescriptor::TYPE_SINT64:
	case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
		return reflection->GetInt64(message, field);
	case google::protobuf::FieldDescriptor::TYPE_UINT32:
	case google::protobuf::FieldDescriptor::TYPE_FIXED32:
		return reflection->GetUInt32(message, field);
	case google::protobuf::FieldDescriptor::TYPE_UINT64:
	case google::protobuf::FieldDescriptor::TYPE_FIXED64:
		return reflection->GetUInt64(message, field);
	case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
		return reflection->GetDouble(message, field);
	case google::protobuf::FieldDescriptor::TYPE_FLOAT:
		return reflection->GetFloat(message, field);
	case google::protobuf::FieldDescriptor::TYPE_BOOL:
		return reflection->GetBool(message, field);
	case google::protobuf::FieldDescriptor::TYPE_MESSAGE:
	{
		const google::protobuf::Message& sub_message = reflection->GetMessage(message, field);
		const google::protobuf::Descriptor* sub_descriptor = sub_message.GetDescriptor();
		boost::json::object obj;

		const google::protobuf::Reflection* sub_reflection = sub_message.GetReflection();
		for (int i = 0; i < sub_descriptor->field_count(); i++) {
			const google::protobuf::FieldDescriptor* sub_field = sub_descriptor->field(i);
			if (sub_reflection->HasField(sub_message, sub_field)) {
				obj[sub_field->name()] = GetJsonFromProtobufField(sub_message, sub_field);
			}
		}
		return obj;
	}
	default:
		return boost::json::value();
	}
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
	AddMethod(L"GetParameters", L"ПолучитьПараметры", this, &SimpleKafka1C::getParameters);
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
	AddMethod(L"GetTopicMetadata", L"ПолучитьМетаданныеТопика", this, &SimpleKafka1C::getTopicMetadata, { {2, 5000} });
	AddMethod(L"getConsumerCurrentGroupOffset", L"ПолучитьСмещенияТекущегоКонсьюмера", this, &SimpleKafka1C::getConsumerCurrentGroupOffset, { {0, std::string("")}, {1, 5000}});
	AddMethod(L"getConsumerGroupOffsets", L"ПолучитьСмещенияГруппыКонсьюмеров", this, &SimpleKafka1C::getConsumerGroupOffsets, { {1, std::string("")}, {2, 5000}});
	AddMethod(L"CreateTopic", L"СоздатьТопик", this, &SimpleKafka1C::createTopic);
	AddMethod(L"DeleteTopic", L"УдалитьТопик", this, &SimpleKafka1C::deleteTopic);
	AddMethod(L"DeleteRecords", L"УдалитьЗаписи", this, &SimpleKafka1C::deleteRecords, { {3, 10000} });
	AddMethod(L"GetTopicConfig", L"ПолучитьНастройкиТопика", this, &SimpleKafka1C::getTopicConfig, { {2, 5000} });
	AddMethod(L"SetTopicConfig", L"УстановитьНастройкиТопика", this, &SimpleKafka1C::setTopicConfig, { {3, 10000} });
	AddMethod(L"GetConsumerLag", L"ПолучитьОтставаниеКонсьюмера", this, &SimpleKafka1C::getConsumerLag, { {3, 5000} });
	AddMethod(L"GetTopicConsumerGroups", L"ПолучитьКонсьюмеровТопика", this, &SimpleKafka1C::getTopicConsumerGroups, { {2, 5000} });
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
	AddMethod(L"DecodeAvroMessage", L"ДекодироватьСообщениеAVRO", this, &SimpleKafka1C::decodeAvroMessage,
		{ {1, std::string("")}, {2, true} });

	AddMethod(L"PutProtoSchema", L"СохранитьСхемуProtobuf", this, &SimpleKafka1C::putProtoSchema);
	AddMethod(L"ConvertToProtobufFormat", L"ПреобразоватьВФорматProtobuf", this, &SimpleKafka1C::convertToProtobufFormat);
	AddMethod(L"SaveProtobufFile", L"СохранитьФайлProtobuf", this, &SimpleKafka1C::saveProtobufFile);
	AddMethod(L"DecodeProtobufMessage", L"ДекодироватьСообщениеProtobuf", this, &SimpleKafka1C::decodeProtobufMessage,
		{ {1, std::string("")}, {2, true} });
	AddMethod(L"ProduceProtobuf", L"ОтправитьСообщениеProtobuf", this, &SimpleKafka1C::produceProtobuf,
		{ {1, -1}, {3, std::string("")}, {4, std::string("")} });
	AddMethod(L"ProduceProtobufWithWaitResult", L"ОтправитьСообщениеProtobufСОжиданиемРезультата", this, &SimpleKafka1C::produceProtobufWithWaitResult,
		{ {1, -1}, {3, std::string("")}, {4, std::string("")} });

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

std::string SimpleKafka1C::getParameters()
{
	boost::json::object result;
	boost::json::array parametersArray;

	for (const auto& setting : settings)
	{
		boost::json::object paramObj;
		paramObj["key"] = setting.Key;
		paramObj["value"] = setting.Value;
		parametersArray.push_back(paramObj);
	}

	result["parameters"] = parametersArray;
	return boost::json::serialize(result);
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

bool SimpleKafka1C::createTopic(const variant_t& brokers, const variant_t& topicName, const variant_t& partition, const variant_t& replication_factor)
{
	char errstr[512];
	int timeout_ms = 10000;

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);
	int partition_cnt = std::get<int32_t>(partition);
	int rep_factor = std::get<int32_t>(replication_factor);

	// создаем конфигурацию
	rd_kafka_conf_t* conf = rd_kafka_conf_new();

	// дополнительные параметры
	for (size_t i = 0; i < settings.size(); i++)
	{
		if (rd_kafka_conf_set(conf, settings[i].Key.c_str(), settings[i].Value.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			msg_err = errstr;
			rd_kafka_conf_destroy(conf);
			return false;
		}
	}

	if (rd_kafka_conf_set(conf, "bootstrap.servers", tBrokers.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		msg_err = errstr;
		rd_kafka_conf_destroy(conf);
		return false;
	}

	// создаем временного продюсера для Admin API
	rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!rk)
	{
		msg_err = u8"Ошибка создания клиента: " + std::string(errstr);
		return false;
	}

	// создаем очередь для получения результата
	rd_kafka_queue_t* rkqu = rd_kafka_queue_new(rk);

	// создаем описание нового топика
	rd_kafka_NewTopic_t* newt = rd_kafka_NewTopic_new(tTopicName.c_str(), partition_cnt, rep_factor, errstr, sizeof(errstr));
	if (!newt)
	{
		msg_err = u8"Ошибка создания описания топика: " + std::string(errstr);
		rd_kafka_queue_destroy(rkqu);
		rd_kafka_destroy(rk);
		return false;
	}

	// опции операции
	rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_CREATETOPICS);
	rd_kafka_AdminOptions_set_operation_timeout(options, timeout_ms, errstr, sizeof(errstr));

	// выполняем создание топика
	rd_kafka_NewTopic_t* newt_arr[1] = { newt };
	rd_kafka_CreateTopics(rk, newt_arr, 1, options, rkqu);

	// ожидаем результат
	rd_kafka_event_t* rkev = rd_kafka_queue_poll(rkqu, timeout_ms + 2000);

	bool success = false;
	if (rkev)
	{
		if (rd_kafka_event_error(rkev))
		{
			msg_err = rd_kafka_event_error_string(rkev);
		}
		else
		{
			const rd_kafka_CreateTopics_result_t* res = rd_kafka_event_CreateTopics_result(rkev);
			if (res)
			{
				size_t res_cnt;
				const rd_kafka_topic_result_t** terr = rd_kafka_CreateTopics_result_topics(res, &res_cnt);

				if (res_cnt > 0 && terr[0])
				{
					rd_kafka_resp_err_t err = rd_kafka_topic_result_error(terr[0]);
					if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
					{
						msg_err = rd_kafka_topic_result_error_string(terr[0]);
					}
					else
					{
						success = true;
					}
				}
			}
		}
		rd_kafka_event_destroy(rkev);
	}
	else
	{
		msg_err = u8"Таймаут ожидания ответа";
	}

	// очистка ресурсов
	rd_kafka_AdminOptions_destroy(options);
	rd_kafka_NewTopic_destroy(newt);
	rd_kafka_queue_destroy(rkqu);
	rd_kafka_destroy(rk);

	return success;
}

bool SimpleKafka1C::deleteTopic(const variant_t& brokers, const variant_t& topicName)
{
	char errstr[512];
	int timeout_ms = 10000;

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);

	// создаем конфигурацию
	rd_kafka_conf_t* conf = rd_kafka_conf_new();

	// дополнительные параметры
	for (size_t i = 0; i < settings.size(); i++)
	{
		if (rd_kafka_conf_set(conf, settings[i].Key.c_str(), settings[i].Value.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			msg_err = errstr;
			rd_kafka_conf_destroy(conf);
			return false;
		}
	}

	if (rd_kafka_conf_set(conf, "bootstrap.servers", tBrokers.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		msg_err = errstr;
		rd_kafka_conf_destroy(conf);
		return false;
	}

	// создаем временного продюсера для Admin API
	rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!rk)
	{
		msg_err = u8"Ошибка создания клиента: " + std::string(errstr);
		return false;
	}

	// создаем очередь для получения результата
	rd_kafka_queue_t* rkqu = rd_kafka_queue_new(rk);

	// создаем описание топика для удаления
	rd_kafka_DeleteTopic_t* delt = rd_kafka_DeleteTopic_new(tTopicName.c_str());

	// опции операции
	rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DELETETOPICS);
	rd_kafka_AdminOptions_set_operation_timeout(options, timeout_ms, errstr, sizeof(errstr));

	// выполняем удаление топика
	rd_kafka_DeleteTopic_t* delt_arr[1] = { delt };
	rd_kafka_DeleteTopics(rk, delt_arr, 1, options, rkqu);

	// ожидаем результат
	rd_kafka_event_t* rkev = rd_kafka_queue_poll(rkqu, timeout_ms + 2000);

	bool success = false;
	if (rkev)
	{
		if (rd_kafka_event_error(rkev))
		{
			msg_err = rd_kafka_event_error_string(rkev);
		}
		else
		{
			const rd_kafka_DeleteTopics_result_t* res = rd_kafka_event_DeleteTopics_result(rkev);
			if (res)
			{
				size_t res_cnt;
				const rd_kafka_topic_result_t** terr = rd_kafka_DeleteTopics_result_topics(res, &res_cnt);

				if (res_cnt > 0 && terr[0])
				{
					rd_kafka_resp_err_t err = rd_kafka_topic_result_error(terr[0]);
					if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
					{
						msg_err = rd_kafka_topic_result_error_string(terr[0]);
					}
					else
					{
						success = true;
					}
				}
			}
		}
		rd_kafka_event_destroy(rkev);
	}
	else
	{
		msg_err = u8"Таймаут ожидания ответа";
	}

	// очистка ресурсов
	rd_kafka_AdminOptions_destroy(options);
	rd_kafka_DeleteTopic_destroy(delt);
	rd_kafka_queue_destroy(rkqu);
	rd_kafka_destroy(rk);

	return success;
}

bool SimpleKafka1C::deleteRecords(const variant_t& brokers, const variant_t& topicName, const variant_t& partitionsJson, const variant_t& timeout)
{
	char errstr[512];

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);
	std::string tPartitionsJson = std::get<std::string>(partitionsJson);
	int32_t tTimeout = std::get<int32_t>(timeout);

	// парсим JSON с партициями и офсетами
	boost::property_tree::ptree pt;
	std::stringstream ss(tPartitionsJson);

	try {
		boost::property_tree::read_json(ss, pt);
	}
	catch (const std::exception& e) {
		msg_err = u8"Ошибка парсинга JSON: " + std::string(e.what());
		return false;
	}

	// создаем конфигурацию
	rd_kafka_conf_t* conf = rd_kafka_conf_new();

	// дополнительные параметры
	for (size_t i = 0; i < settings.size(); i++)
	{
		if (rd_kafka_conf_set(conf, settings[i].Key.c_str(), settings[i].Value.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			msg_err = errstr;
			rd_kafka_conf_destroy(conf);
			return false;
		}
	}

	if (rd_kafka_conf_set(conf, "bootstrap.servers", tBrokers.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		msg_err = errstr;
		rd_kafka_conf_destroy(conf);
		return false;
	}

	// создаем временного продюсера для Admin API
	rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!rk)
	{
		msg_err = u8"Ошибка создания клиента: " + std::string(errstr);
		return false;
	}

	// создаем список партиций и офсетов для удаления
	rd_kafka_topic_partition_list_t* partitions = rd_kafka_topic_partition_list_new(1);

	try {
		for (const auto& partition_node : pt.get_child("partitions"))
		{
			int32_t partition = partition_node.second.get<int32_t>("partition");
			int64_t offset = partition_node.second.get<int64_t>("offset");

			rd_kafka_topic_partition_list_add(partitions, tTopicName.c_str(), partition)->offset = offset;
		}
	}
	catch (const std::exception& e) {
		msg_err = u8"Ошибка чтения данных партиций: " + std::string(e.what());
		rd_kafka_topic_partition_list_destroy(partitions);
		rd_kafka_destroy(rk);
		return false;
	}

	if (partitions->cnt == 0)
	{
		msg_err = u8"Не указаны партиции для удаления записей";
		rd_kafka_topic_partition_list_destroy(partitions);
		rd_kafka_destroy(rk);
		return false;
	}

	// создаем очередь для получения результата
	rd_kafka_queue_t* rkqu = rd_kafka_queue_new(rk);

	// создаем описание для удаления записей
	rd_kafka_DeleteRecords_t* delr = rd_kafka_DeleteRecords_new(partitions);

	// опции операции
	rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DELETERECORDS);
	rd_kafka_AdminOptions_set_operation_timeout(options, tTimeout, errstr, sizeof(errstr));

	// выполняем удаление записей
	rd_kafka_DeleteRecords_t* delr_arr[1] = { delr };
	rd_kafka_DeleteRecords(rk, delr_arr, 1, options, rkqu);

	// ожидаем результат
	rd_kafka_event_t* rkev = rd_kafka_queue_poll(rkqu, tTimeout + 2000);

	bool success = false;
	if (rkev)
	{
		if (rd_kafka_event_error(rkev))
		{
			msg_err = rd_kafka_event_error_string(rkev);
		}
		else
		{
			const rd_kafka_DeleteRecords_result_t* res = rd_kafka_event_DeleteRecords_result(rkev);
			if (res)
			{
				const rd_kafka_topic_partition_list_t* offsets = rd_kafka_DeleteRecords_result_offsets(res);

				if (offsets && offsets->cnt > 0)
				{
					// проверяем результаты для каждой партиции
					bool all_ok = true;
					std::stringstream error_details;

					for (int i = 0; i < offsets->cnt; i++)
					{
						const rd_kafka_topic_partition_t* part = &offsets->elems[i];
						if (part->err != RD_KAFKA_RESP_ERR_NO_ERROR)
						{
							all_ok = false;
							error_details << u8"Партиция " << part->partition
								<< u8": " << rd_kafka_err2str(part->err) << "; ";
						}
					}

					if (all_ok)
					{
						success = true;
					}
					else
					{
						msg_err = error_details.str();
					}
				}
				else
				{
					msg_err = u8"Пустой результат удаления записей";
				}
			}
		}
		rd_kafka_event_destroy(rkev);
	}
	else
	{
		msg_err = u8"Таймаут ожидания ответа";
	}

	// очистка ресурсов
	rd_kafka_AdminOptions_destroy(options);
	rd_kafka_DeleteRecords_destroy(delr);
	rd_kafka_topic_partition_list_destroy(partitions);
	rd_kafka_queue_destroy(rkqu);
	rd_kafka_destroy(rk);

	return success;
}

std::string SimpleKafka1C::getTopicConfig(const variant_t& brokers, const variant_t& topicName, const variant_t& timeout)
{
	std::string result;
	std::stringstream s{};
	char errstr[512];

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);
	int32_t tTimeout = std::get<int32_t>(timeout);

	// создаем конфигурацию
	rd_kafka_conf_t* conf = rd_kafka_conf_new();

	// дополнительные параметры
	for (size_t i = 0; i < settings.size(); i++)
	{
		if (rd_kafka_conf_set(conf, settings[i].Key.c_str(), settings[i].Value.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			msg_err = errstr;
			rd_kafka_conf_destroy(conf);
			return result;
		}
	}

	if (rd_kafka_conf_set(conf, "bootstrap.servers", tBrokers.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		msg_err = errstr;
		rd_kafka_conf_destroy(conf);
		return result;
	}

	// создаем временного продюсера для Admin API
	rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!rk)
	{
		msg_err = u8"Ошибка создания клиента: " + std::string(errstr);
		return result;
	}

	// создаем очередь для получения результата
	rd_kafka_queue_t* rkqu = rd_kafka_queue_new(rk);

	// создаем ресурс конфигурации для топика
	rd_kafka_ConfigResource_t* config_resource = rd_kafka_ConfigResource_new(
		RD_KAFKA_RESOURCE_TOPIC, tTopicName.c_str());

	// опции операции
	rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS);
	rd_kafka_AdminOptions_set_operation_timeout(options, tTimeout, errstr, sizeof(errstr));

	// выполняем запрос конфигурации
	rd_kafka_ConfigResource_t* config_arr[1] = { config_resource };
	rd_kafka_DescribeConfigs(rk, config_arr, 1, options, rkqu);

	// ожидаем результат
	rd_kafka_event_t* rkev = rd_kafka_queue_poll(rkqu, tTimeout + 2000);

	if (rkev)
	{
		if (rd_kafka_event_error(rkev))
		{
			msg_err = rd_kafka_event_error_string(rkev);
		}
		else
		{
			const rd_kafka_DescribeConfigs_result_t* res = rd_kafka_event_DescribeConfigs_result(rkev);
			if (res)
			{
				size_t res_cnt;
				const rd_kafka_ConfigResource_t** resources = rd_kafka_DescribeConfigs_result_resources(res, &res_cnt);

				if (res_cnt > 0)
				{
					boost::property_tree::ptree jsonObj;
					boost::property_tree::ptree configChildren;

					rd_kafka_resp_err_t err = rd_kafka_ConfigResource_error(resources[0]);
					if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
					{
						msg_err = rd_kafka_ConfigResource_error_string(resources[0]);
					}
					else
					{
						jsonObj.put("topic", tTopicName);

						size_t config_cnt;
						const rd_kafka_ConfigEntry_t** entries = rd_kafka_ConfigResource_configs(resources[0], &config_cnt);

						for (size_t i = 0; i < config_cnt; i++)
						{
							const char* name = rd_kafka_ConfigEntry_name(entries[i]);
							const char* value = rd_kafka_ConfigEntry_value(entries[i]);
							rd_kafka_ConfigSource_t source = rd_kafka_ConfigEntry_source(entries[i]);
							int is_read_only = rd_kafka_ConfigEntry_is_read_only(entries[i]);
							int is_default = rd_kafka_ConfigEntry_is_default(entries[i]);
							int is_sensitive = rd_kafka_ConfigEntry_is_sensitive(entries[i]);

							boost::property_tree::ptree node;
							node.put("name", name ? name : "");
							node.put("value", (value && !is_sensitive) ? value : "");
							node.put("is_read_only", is_read_only ? true : false);
							node.put("is_default", is_default ? true : false);
							node.put("is_sensitive", is_sensitive ? true : false);

							std::string source_str;
							switch (source)
							{
							case RD_KAFKA_CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG:
								source_str = "DYNAMIC_TOPIC_CONFIG";
								break;
							case RD_KAFKA_CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG:
								source_str = "DYNAMIC_BROKER_CONFIG";
								break;
							case RD_KAFKA_CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG:
								source_str = "DYNAMIC_DEFAULT_BROKER_CONFIG";
								break;
							case RD_KAFKA_CONFIG_SOURCE_STATIC_BROKER_CONFIG:
								source_str = "STATIC_BROKER_CONFIG";
								break;
							case RD_KAFKA_CONFIG_SOURCE_DEFAULT_CONFIG:
								source_str = "DEFAULT_CONFIG";
								break;
							default:
								source_str = "UNKNOWN";
								break;
							}
							node.put("source", source_str);

							configChildren.push_back(boost::property_tree::ptree::value_type("", node));
						}

						if (configChildren.size())
						{
							jsonObj.put_child("configs", configChildren);
						}

						boost::property_tree::write_json(s, jsonObj, true);
						result = s.str();
					}
				}
			}
		}
		rd_kafka_event_destroy(rkev);
	}
	else
	{
		msg_err = u8"Таймаут ожидания ответа";
	}

	// очистка ресурсов
	rd_kafka_AdminOptions_destroy(options);
	rd_kafka_ConfigResource_destroy(config_resource);
	rd_kafka_queue_destroy(rkqu);
	rd_kafka_destroy(rk);

	return result;
}

bool SimpleKafka1C::setTopicConfig(const variant_t& brokers, const variant_t& topicName, const variant_t& configJson, const variant_t& timeout)
{
	char errstr[512];

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);
	std::string tConfigJson = std::get<std::string>(configJson);
	int32_t tTimeout = std::get<int32_t>(timeout);

	// создаем конфигурацию
	rd_kafka_conf_t* conf = rd_kafka_conf_new();

	// дополнительные параметры
	for (size_t i = 0; i < settings.size(); i++)
	{
		if (rd_kafka_conf_set(conf, settings[i].Key.c_str(), settings[i].Value.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			msg_err = errstr;
			rd_kafka_conf_destroy(conf);
			return false;
		}
	}

	if (rd_kafka_conf_set(conf, "bootstrap.servers", tBrokers.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		msg_err = errstr;
		rd_kafka_conf_destroy(conf);
		return false;
	}

	// создаем временного продюсера для Admin API
	rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!rk)
	{
		msg_err = u8"Ошибка создания клиента: " + std::string(errstr);
		return false;
	}

	// создаем очередь для получения результата
	rd_kafka_queue_t* rkqu = rd_kafka_queue_new(rk);

	// создаем ресурс конфигурации для топика
	rd_kafka_ConfigResource_t* config_resource = rd_kafka_ConfigResource_new(
		RD_KAFKA_RESOURCE_TOPIC, tTopicName.c_str());

	// парсим JSON с настройками
	// Формат: {"retention.ms": "86400000", "cleanup.policy": "delete"}
	try
	{
		boost::json::value parsed = boost::json::parse(tConfigJson);
		boost::json::object obj = parsed.as_object();

		for (auto& kv : obj)
		{
			std::string key = kv.key();
			std::string value;

			if (kv.value().is_string())
			{
				value = kv.value().as_string();
			}
			else if (kv.value().is_int64())
			{
				value = std::to_string(kv.value().as_int64());
			}
			else if (kv.value().is_bool())
			{
				value = kv.value().as_bool() ? "true" : "false";
			}
			else if (kv.value().is_double())
			{
				value = std::to_string(kv.value().as_double());
			}
			else
			{
				continue;
			}

			rd_kafka_ConfigResource_set_config(config_resource, key.c_str(), value.c_str());
		}
	}
	catch (std::exception const& ex)
	{
		msg_err = u8"Ошибка парсинга JSON: " + std::string(ex.what());
		rd_kafka_ConfigResource_destroy(config_resource);
		rd_kafka_queue_destroy(rkqu);
		rd_kafka_destroy(rk);
		return false;
	}

	// опции операции
	rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ALTERCONFIGS);
	rd_kafka_AdminOptions_set_operation_timeout(options, tTimeout, errstr, sizeof(errstr));

	// выполняем изменение конфигурации
	rd_kafka_ConfigResource_t* config_arr[1] = { config_resource };
	rd_kafka_AlterConfigs(rk, config_arr, 1, options, rkqu);

	// ожидаем результат
	rd_kafka_event_t* rkev = rd_kafka_queue_poll(rkqu, tTimeout + 2000);

	bool success = false;
	if (rkev)
	{
		if (rd_kafka_event_error(rkev))
		{
			msg_err = rd_kafka_event_error_string(rkev);
		}
		else
		{
			const rd_kafka_AlterConfigs_result_t* res = rd_kafka_event_AlterConfigs_result(rkev);
			if (res)
			{
				size_t res_cnt;
				const rd_kafka_ConfigResource_t** resources = rd_kafka_AlterConfigs_result_resources(res, &res_cnt);

				if (res_cnt > 0)
				{
					rd_kafka_resp_err_t err = rd_kafka_ConfigResource_error(resources[0]);
					if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
					{
						msg_err = rd_kafka_ConfigResource_error_string(resources[0]);
					}
					else
					{
						success = true;
					}
				}
			}
		}
		rd_kafka_event_destroy(rkev);
	}
	else
	{
		msg_err = u8"Таймаут ожидания ответа";
	}

	// очистка ресурсов
	rd_kafka_AdminOptions_destroy(options);
	rd_kafka_ConfigResource_destroy(config_resource);
	rd_kafka_queue_destroy(rkqu);
	rd_kafka_destroy(rk);

	return success;
}

std::string SimpleKafka1C::getConsumerLag(const variant_t& brokers, const variant_t& topicName, const variant_t& consumerGroup, const variant_t& timeout)
{
	std::string result;
	std::stringstream s{};
	char errstr[512];

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);
	std::string tConsumerGroup = std::get<std::string>(consumerGroup);
	int32_t tTimeout = std::get<int32_t>(timeout);

	// создаем конфигурацию
	rd_kafka_conf_t* conf = rd_kafka_conf_new();

	// дополнительные параметры
	for (size_t i = 0; i < settings.size(); i++)
	{
		if (rd_kafka_conf_set(conf, settings[i].Key.c_str(), settings[i].Value.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			msg_err = errstr;
			rd_kafka_conf_destroy(conf);
			return result;
		}
	}

	if (rd_kafka_conf_set(conf, "bootstrap.servers", tBrokers.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		msg_err = errstr;
		rd_kafka_conf_destroy(conf);
		return result;
	}

	if (rd_kafka_conf_set(conf, "group.id", tConsumerGroup.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		msg_err = errstr;
		rd_kafka_conf_destroy(conf);
		return result;
	}

	// создаем консьюмера для Admin API
	rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
	if (!rk)
	{
		msg_err = u8"Ошибка создания клиента: " + std::string(errstr);
		return result;
	}

	// получаем метаданные топика для определения партиций
	const rd_kafka_metadata_t* metadata;
	rd_kafka_topic_t* rkt = rd_kafka_topic_new(rk, tTopicName.c_str(), nullptr);
	if (!rkt)
	{
		msg_err = u8"Ошибка создания дескриптора топика";
		rd_kafka_destroy(rk);
		return result;
	}

	rd_kafka_resp_err_t err = rd_kafka_metadata(rk, 0, rkt, &metadata, tTimeout);
	if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
	{
		msg_err = rd_kafka_err2str(err);
		rd_kafka_topic_destroy(rkt);
		rd_kafka_destroy(rk);
		return result;
	}

	// находим нужный топик в метаданных
	const rd_kafka_metadata_topic_t* topic_metadata = nullptr;
	for (int i = 0; i < metadata->topic_cnt; i++)
	{
		if (strcmp(metadata->topics[i].topic, tTopicName.c_str()) == 0)
		{
			topic_metadata = &metadata->topics[i];
			break;
		}
	}

	if (!topic_metadata)
	{
		msg_err = u8"Топик не найден";
		rd_kafka_metadata_destroy(metadata);
		rd_kafka_topic_destroy(rkt);
		rd_kafka_destroy(rk);
		return result;
	}

	int partition_cnt = topic_metadata->partition_cnt;

	// создаем список партиций для запроса committed offsets
	rd_kafka_topic_partition_list_t* partitions = rd_kafka_topic_partition_list_new(partition_cnt);
	for (int i = 0; i < partition_cnt; i++)
	{
		rd_kafka_topic_partition_list_add(partitions, tTopicName.c_str(), metadata->topics[0].partitions[i].id);
	}

	// получаем committed offsets для группы консьюмеров
	err = rd_kafka_committed(rk, partitions, tTimeout);
	if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
	{
		msg_err = rd_kafka_err2str(err);
		rd_kafka_topic_partition_list_destroy(partitions);
		rd_kafka_metadata_destroy(metadata);
		rd_kafka_topic_destroy(rkt);
		rd_kafka_destroy(rk);
		return result;
	}

	// формируем результат
	boost::property_tree::ptree jsonObj;
	boost::property_tree::ptree partitionsChildren;
	int64_t totalLag = 0;

	jsonObj.put("topic", tTopicName);
	jsonObj.put("consumer_group", tConsumerGroup);

	for (int i = 0; i < partitions->cnt; i++)
	{
		rd_kafka_topic_partition_t* part = &partitions->elems[i];

		// получаем watermark offsets для партиции
		int64_t low = 0, high = 0;
		err = rd_kafka_query_watermark_offsets(rk, tTopicName.c_str(), part->partition, &low, &high, tTimeout);

		boost::property_tree::ptree partNode;
		partNode.put("partition", part->partition);
		partNode.put("low_watermark", low);
		partNode.put("high_watermark", high);

		if (part->offset >= 0)
		{
			partNode.put("committed_offset", part->offset);
			int64_t lag = high - part->offset;
			if (lag < 0) lag = 0;
			partNode.put("lag", lag);
			totalLag += lag;
		}
		else
		{
			// offset не зафиксирован (-1001 = RD_KAFKA_OFFSET_INVALID)
			partNode.put("committed_offset", "none");
			partNode.put("lag", high - low);
			totalLag += (high - low);
		}

		if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
		{
			partNode.put("watermark_error", rd_kafka_err2str(err));
		}

		partitionsChildren.push_back(boost::property_tree::ptree::value_type("", partNode));
	}

	jsonObj.put("total_lag", totalLag);

	if (partitionsChildren.size())
	{
		jsonObj.put_child("partitions", partitionsChildren);
	}

	boost::property_tree::write_json(s, jsonObj, true);
	result = s.str();

	// очистка ресурсов
	rd_kafka_topic_partition_list_destroy(partitions);
	rd_kafka_metadata_destroy(metadata);
	rd_kafka_topic_destroy(rkt);
	rd_kafka_destroy(rk);

	return result;
}

std::string SimpleKafka1C::getTopicConsumerGroups(const variant_t& brokers, const variant_t& topicName, const variant_t& timeout)
{
	std::string result;
	std::stringstream s{};
	char errstr[512];

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);
	int32_t tTimeout = std::get<int32_t>(timeout);

	// создаем конфигурацию
	rd_kafka_conf_t* conf = rd_kafka_conf_new();

	// дополнительные параметры
	for (size_t i = 0; i < settings.size(); i++)
	{
		if (rd_kafka_conf_set(conf, settings[i].Key.c_str(), settings[i].Value.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			msg_err = errstr;
			rd_kafka_conf_destroy(conf);
			return result;
		}
	}

	if (rd_kafka_conf_set(conf, "bootstrap.servers", tBrokers.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		msg_err = errstr;
		rd_kafka_conf_destroy(conf);
		return result;
	}

	// создаем клиента для Admin API
	rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!rk)
	{
		msg_err = u8"Ошибка создания клиента: " + std::string(errstr);
		return result;
	}

	// создаем очередь для получения результатов
	rd_kafka_queue_t* rkqu = rd_kafka_queue_new(rk);

	// опции для операции ListConsumerGroups
	rd_kafka_AdminOptions_t* list_options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS);
	rd_kafka_AdminOptions_set_request_timeout(list_options, tTimeout, errstr, sizeof(errstr));

	// получаем список всех consumer groups
	rd_kafka_ListConsumerGroups(rk, list_options, rkqu);

	// ожидаем результат
	rd_kafka_event_t* rkev = rd_kafka_queue_poll(rkqu, tTimeout + 2000);
	if (!rkev)
	{
		msg_err = u8"Таймаут при получении списка групп";
		rd_kafka_AdminOptions_destroy(list_options);
		rd_kafka_queue_destroy(rkqu);
		rd_kafka_destroy(rk);
		return result;
	}

	if (rd_kafka_event_error(rkev))
	{
		msg_err = rd_kafka_event_error_string(rkev);
		rd_kafka_event_destroy(rkev);
		rd_kafka_AdminOptions_destroy(list_options);
		rd_kafka_queue_destroy(rkqu);
		rd_kafka_destroy(rk);
		return result;
	}

	const rd_kafka_ListConsumerGroups_result_t* list_result = rd_kafka_event_ListConsumerGroups_result(rkev);
	if (!list_result)
	{
		msg_err = u8"Не удалось получить результат списка групп";
		rd_kafka_event_destroy(rkev);
		rd_kafka_AdminOptions_destroy(list_options);
		rd_kafka_queue_destroy(rkqu);
		rd_kafka_destroy(rk);
		return result;
	}

	// получаем список групп и их состояния
	size_t valid_cnt = 0;
	const rd_kafka_ConsumerGroupListing_t** listings = rd_kafka_ListConsumerGroups_result_valid(list_result, &valid_cnt);

	struct GroupInfo {
		std::string group_id;
		rd_kafka_consumer_group_state_t state;
	};
	std::vector<GroupInfo> allGroups;

	for (size_t i = 0; i < valid_cnt; i++)
	{
		const char* group_id = rd_kafka_ConsumerGroupListing_group_id(listings[i]);
		if (group_id)
		{
			GroupInfo info;
			info.group_id = group_id;
			info.state = rd_kafka_ConsumerGroupListing_state(listings[i]);
			allGroups.push_back(info);
		}
	}

	rd_kafka_event_destroy(rkev);
	rd_kafka_AdminOptions_destroy(list_options);

	if (allGroups.empty())
	{
		// Нет групп - возвращаем пустой результат
		boost::property_tree::ptree jsonObj;
		jsonObj.put("topic", tTopicName);
		jsonObj.put("groups_count", 0);
		boost::property_tree::ptree emptyGroups;
		jsonObj.put_child("consumer_groups", emptyGroups);
		boost::property_tree::write_json(s, jsonObj, true);
		rd_kafka_queue_destroy(rkqu);
		rd_kafka_destroy(rk);
		return s.str();
	}

	// получаем метаданные топика для определения партиций
	const rd_kafka_metadata_t* metadata;
	rd_kafka_topic_t* rkt = rd_kafka_topic_new(rk, tTopicName.c_str(), nullptr);
	if (!rkt)
	{
		msg_err = u8"Ошибка создания дескриптора топика";
		rd_kafka_queue_destroy(rkqu);
		rd_kafka_destroy(rk);
		return result;
	}

	rd_kafka_resp_err_t err = rd_kafka_metadata(rk, 0, rkt, &metadata, tTimeout);
	if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
	{
		msg_err = rd_kafka_err2str(err);
		rd_kafka_topic_destroy(rkt);
		rd_kafka_queue_destroy(rkqu);
		rd_kafka_destroy(rk);
		return result;
	}

	// находим количество партиций топика
	int partition_cnt = 0;
	for (int i = 0; i < metadata->topic_cnt; i++)
	{
		if (strcmp(metadata->topics[i].topic, tTopicName.c_str()) == 0)
		{
			partition_cnt = metadata->topics[i].partition_cnt;
			break;
		}
	}

	rd_kafka_metadata_destroy(metadata);
	rd_kafka_topic_destroy(rkt);

	if (partition_cnt == 0)
	{
		msg_err = u8"Топик не найден или не имеет партиций";
		rd_kafka_queue_destroy(rkqu);
		rd_kafka_destroy(rk);
		return result;
	}

	// формируем результат
	boost::property_tree::ptree jsonObj;
	boost::property_tree::ptree groupsChildren;

	jsonObj.put("topic", tTopicName);

	// для каждой группы проверяем наличие committed offsets для топика
	for (size_t i = 0; i < allGroups.size(); i++)
	{
		const std::string& group_id = allGroups[i].group_id;

		// создаем список партиций для запроса
		rd_kafka_topic_partition_list_t* partitions = rd_kafka_topic_partition_list_new(partition_cnt);
		for (int p = 0; p < partition_cnt; p++)
		{
			rd_kafka_topic_partition_list_add(partitions, tTopicName.c_str(), p);
		}

		// создаем запрос ListConsumerGroupOffsets
		rd_kafka_ListConsumerGroupOffsets_t* grp_offsets = rd_kafka_ListConsumerGroupOffsets_new(group_id.c_str(), partitions);
		rd_kafka_ListConsumerGroupOffsets_t* grp_offsets_arr[1] = { grp_offsets };

		rd_kafka_AdminOptions_t* offsets_options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS);
		rd_kafka_AdminOptions_set_request_timeout(offsets_options, tTimeout, errstr, sizeof(errstr));

		rd_kafka_ListConsumerGroupOffsets(rk, grp_offsets_arr, 1, offsets_options, rkqu);

		rd_kafka_event_t* offset_ev = rd_kafka_queue_poll(rkqu, tTimeout + 1000);

		bool hasOffsets = false;
		boost::property_tree::ptree offsetsChildren;

		if (offset_ev)
		{
			if (!rd_kafka_event_error(offset_ev))
			{
				const rd_kafka_ListConsumerGroupOffsets_result_t* offset_result =
					rd_kafka_event_ListConsumerGroupOffsets_result(offset_ev);

				if (offset_result)
				{
					size_t res_cnt = 0;
					const rd_kafka_group_result_t** group_results =
						rd_kafka_ListConsumerGroupOffsets_result_groups(offset_result, &res_cnt);

					if (res_cnt > 0 && group_results[0])
					{
						const rd_kafka_topic_partition_list_t* result_partitions =
							rd_kafka_group_result_partitions(group_results[0]);

						if (result_partitions)
						{
							for (int p = 0; p < result_partitions->cnt; p++)
							{
								// offset >= 0 означает что группа имеет committed offset для этой партиции
								if (result_partitions->elems[p].offset >= 0)
								{
									hasOffsets = true;

									boost::property_tree::ptree offsetNode;
									offsetNode.put("partition", result_partitions->elems[p].partition);
									offsetNode.put("offset", result_partitions->elems[p].offset);
									offsetsChildren.push_back(boost::property_tree::ptree::value_type("", offsetNode));
								}
							}
						}
					}
				}
			}
			rd_kafka_event_destroy(offset_ev);
		}

		rd_kafka_AdminOptions_destroy(offsets_options);
		rd_kafka_ListConsumerGroupOffsets_destroy(grp_offsets);
		rd_kafka_topic_partition_list_destroy(partitions);

		// если группа имеет offsets для этого топика - добавляем её в результат
		if (hasOffsets)
		{
			boost::property_tree::ptree groupNode;
			groupNode.put("group_id", group_id);
			groupNode.put("state", rd_kafka_consumer_group_state_name(allGroups[i].state));

			if (offsetsChildren.size())
			{
				groupNode.put_child("offsets", offsetsChildren);
			}

			groupsChildren.push_back(boost::property_tree::ptree::value_type("", groupNode));
		}
	}

	jsonObj.put("groups_count", groupsChildren.size());
	jsonObj.put_child("consumer_groups", groupsChildren);

	boost::property_tree::write_json(s, jsonObj, true);
	result = s.str();

	// очистка ресурсов
	rd_kafka_queue_destroy(rkqu);
	rd_kafka_destroy(rk);

	return result;
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

std::string SimpleKafka1C::getTopicMetadata(const variant_t& brokers, const variant_t& topicName, const variant_t& timeout)
{
	std::string result;
	std::stringstream s{};

	boost::property_tree::ptree jsonObj;
	RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);
	int32_t tTimeout = std::get<int32_t>(timeout);

	// дополнительные параметры
	for (size_t i = 0; i < settings.size(); i++)
	{
		if (conf->set(settings[i].Key, settings[i].Value, msg_err) != RdKafka::Conf::CONF_OK)
		{
			delete conf;
			return result;
		}
	}

	if (conf->set("metadata.broker.list", tBrokers, msg_err) != RdKafka::Conf::CONF_OK)
	{
		delete conf;
		return result;
	}

	// создаем временного продюсера для получения метаданных
	RdKafka::Producer* producer = RdKafka::Producer::create(conf, msg_err);
	if (!producer)
	{
		msg_err = u8"Ошибка создания временного продюсера";
		delete conf;
		return result;
	}

	// получаем метаданные для конкретного топика
	RdKafka::Topic* topicHandle = RdKafka::Topic::create(producer, tTopicName, nullptr, msg_err);
	if (!topicHandle)
	{
		msg_err = u8"Ошибка создания дескриптора топика: " + msg_err;
		delete producer;
		delete conf;
		return result;
	}

	class RdKafka::Metadata* metadata;
	RdKafka::ErrorCode err = producer->metadata(false, topicHandle, &metadata, tTimeout);
	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(err);
		delete topicHandle;
		delete producer;
		delete conf;
		return result;
	}

	// информация о брокерах
	boost::property_tree::ptree brokersChildren;
	RdKafka::Metadata::BrokerMetadataIterator brokerIt;
	for (brokerIt = metadata->brokers()->begin(); brokerIt != metadata->brokers()->end(); ++brokerIt)
	{
		boost::property_tree::ptree brokerNode;
		brokerNode.put("id", (*brokerIt)->id());
		brokerNode.put("host", (*brokerIt)->host());
		brokerNode.put("port", (*brokerIt)->port());
		brokersChildren.push_back(boost::property_tree::ptree::value_type("", brokerNode));
	}

	if (brokersChildren.size())
	{
		jsonObj.put_child("brokers", brokersChildren);
	}

	// информация о топике и его партициях
	RdKafka::Metadata::TopicMetadataIterator topicIt;
	for (topicIt = metadata->topics()->begin(); topicIt != metadata->topics()->end(); ++topicIt)
	{
		if ((*topicIt)->topic() == tTopicName)
		{
			jsonObj.put("topic", (*topicIt)->topic());

			if ((*topicIt)->err() != RdKafka::ERR_NO_ERROR)
			{
				jsonObj.put("error", RdKafka::err2str((*topicIt)->err()));
			}

			boost::property_tree::ptree partitionsChildren;
			typedef RdKafka::TopicMetadata::PartitionMetadataIterator PartitionIterator;

			for (PartitionIterator partIt = (*topicIt)->partitions()->begin();
				 partIt != (*topicIt)->partitions()->end(); ++partIt)
			{
				boost::property_tree::ptree partNode;
				partNode.put("id", (*partIt)->id());
				partNode.put("leader", (*partIt)->leader());

				if ((*partIt)->err() != RdKafka::ERR_NO_ERROR)
				{
					partNode.put("error", RdKafka::err2str((*partIt)->err()));
				}

				// реплики
				boost::property_tree::ptree replicasChildren;
				const std::vector<int32_t>* replicas = (*partIt)->replicas();
				for (size_t r = 0; r < replicas->size(); r++)
				{
					boost::property_tree::ptree replicaNode;
					replicaNode.put("", (*replicas)[r]);
					replicasChildren.push_back(boost::property_tree::ptree::value_type("", replicaNode));
				}
				if (replicasChildren.size())
				{
					partNode.put_child("replicas", replicasChildren);
				}

				// ISR (In-Sync Replicas)
				boost::property_tree::ptree isrsChildren;
				const std::vector<int32_t>* isrs = (*partIt)->isrs();
				for (size_t isr = 0; isr < isrs->size(); isr++)
				{
					boost::property_tree::ptree isrNode;
					isrNode.put("", (*isrs)[isr]);
					isrsChildren.push_back(boost::property_tree::ptree::value_type("", isrNode));
				}
				if (isrsChildren.size())
				{
					partNode.put_child("isrs", isrsChildren);
				}

				partitionsChildren.push_back(boost::property_tree::ptree::value_type("", partNode));
			}

			if (partitionsChildren.size())
			{
				jsonObj.put("partitions_count", (*topicIt)->partitions()->size());
				jsonObj.put_child("partitions", partitionsChildren);
			}

			break;
		}
	}

	delete metadata;
	delete topicHandle;
	delete producer;
	delete conf;

	boost::property_tree::write_json(s, jsonObj, true);
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
						// Поддержка UUID (логический тип над string)
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
					case avro::AVRO_FIXED:
						// Поддержка FIXED (может использоваться для UUID как 16 байт)
						if (field->value().is_null())
						{
							fieldDatum.selectBranch(0);
						}
						else
						{
							avro::GenericFixed& fixed = fieldDatum.value<avro::GenericFixed>();
							std::string strVal = std::string(field->value().as_string());
							// Если это UUID в строковом формате, конвертируем в байты
							if (strVal.length() == 36 && strVal[8] == '-' && strVal[13] == '-')
							{
								// UUID формат: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
								std::string hex;
								for (char c : strVal)
								{
									if (c != '-') hex += c;
								}
								std::vector<uint8_t>& bytes = fixed.value();
								bytes.resize(16);
								for (size_t i = 0; i < 16; i++)
								{
									std::string byteStr = hex.substr(i * 2, 2);
									bytes[i] = static_cast<uint8_t>(std::stoul(byteStr, nullptr, 16));
								}
							}
							else
							{
								// Копируем как есть
								std::vector<uint8_t>& bytes = fixed.value();
								bytes.assign(strVal.begin(), strVal.end());
							}
						}
						break;
					case avro::AVRO_BYTES:
						if (field->value().is_null())
							fieldDatum.selectBranch(0);
						else
						{
							std::string strVal = std::string(field->value().as_string());
							std::vector<uint8_t> bytes(strVal.begin(), strVal.end());
							fieldDatum.value<std::vector<uint8_t>>() = bytes;
						}
						break;
					default:
						msg_err += u8"Unsupported type '" + type + u8"' on '" + key + u8"'. Supported: AVRO_STRING, AVRO_LONG, AVRO_INT, AVRO_FLOAT, AVRO_DOUBLE, AVRO_BOOL, AVRO_NULL, AVRO_UNION, AVRO_FIXED, AVRO_BYTES. ";
						break;
					}
				}
				else
				{
					switch (fieldDatum.type())
					{
					case avro::AVRO_STRING:
						// Поддержка UUID (логический тип над string)
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
					case avro::AVRO_FIXED:
						{
							// Поддержка FIXED (может использоваться для UUID как 16 байт)
							avro::GenericFixed& fixed = fieldDatum.value<avro::GenericFixed>();
							std::string strVal = std::string(field->value().as_string());
							// Если это UUID в строковом формате, конвертируем в байты
							if (strVal.length() == 36 && strVal[8] == '-' && strVal[13] == '-')
							{
								// UUID формат: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
								std::string hex;
								for (char c : strVal)
								{
									if (c != '-') hex += c;
								}
								std::vector<uint8_t>& bytes = fixed.value();
								bytes.resize(16);
								for (size_t i = 0; i < 16; i++)
								{
									std::string byteStr = hex.substr(i * 2, 2);
									bytes[i] = static_cast<uint8_t>(std::stoul(byteStr, nullptr, 16));
								}
							}
							else
							{
								// Копируем как есть
								std::vector<uint8_t>& bytes = fixed.value();
								bytes.assign(strVal.begin(), strVal.end());
							}
						}
						break;
					case avro::AVRO_BYTES:
						{
							std::string strVal = std::string(field->value().as_string());
							std::vector<uint8_t> bytes(strVal.begin(), strVal.end());
							fieldDatum.value<std::vector<uint8_t>>() = bytes;
						}
						break;
					default:
						msg_err += "Unsupported type '" + type + "' on '" + key + "'. Supported: AVRO_STRING, AVRO_LONG, AVRO_INT, AVRO_FLOAT, AVRO_DOUBLE, AVRO_BOOL, AVRO_NULL, AVRO_UNION, AVRO_FIXED, AVRO_BYTES. ";
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

static boost::json::value convertAvroDatumToJson(const avro::GenericDatum& datum)
{
	if (datum.isUnion())
	{
		const avro::GenericDatum& actualDatum = datum.value<avro::GenericUnion>().datum();
		if (actualDatum.type() == avro::AVRO_NULL)
		{
			return nullptr;
		}
		return convertAvroDatumToJson(actualDatum);
	}

	switch (datum.type())
	{
	case avro::AVRO_STRING:
		return boost::json::string(datum.value<std::string>());
	case avro::AVRO_LONG:
		return datum.value<int64_t>();
	case avro::AVRO_INT:
		return static_cast<int64_t>(datum.value<int32_t>());
	case avro::AVRO_FLOAT:
		return static_cast<double>(datum.value<float>());
	case avro::AVRO_DOUBLE:
		return datum.value<double>();
	case avro::AVRO_BOOL:
		return datum.value<bool>();
	case avro::AVRO_NULL:
		return nullptr;
	case avro::AVRO_BYTES:
	{
		const std::vector<uint8_t>& bytes = datum.value<std::vector<uint8_t>>();
		std::string str(bytes.begin(), bytes.end());
		return boost::json::string(str);
	}
	case avro::AVRO_FIXED:
	{
		const avro::GenericFixed& fixed = datum.value<avro::GenericFixed>();
		const std::vector<uint8_t>& bytes = fixed.value();
		// Если это 16 байт, считаем что UUID
		if (bytes.size() == 16)
		{
			char uuidStr[37];
			snprintf(uuidStr, sizeof(uuidStr),
				"%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
				bytes[0], bytes[1], bytes[2], bytes[3],
				bytes[4], bytes[5], bytes[6], bytes[7],
				bytes[8], bytes[9], bytes[10], bytes[11],
				bytes[12], bytes[13], bytes[14], bytes[15]);
			return boost::json::string(uuidStr);
		}
		else
		{
			std::string str(bytes.begin(), bytes.end());
			return boost::json::string(str);
		}
	}
	default:
		return nullptr;
	}
}

variant_t SimpleKafka1C::decodeAvroMessage(const variant_t& avroData, const variant_t& schemaJsonName, const variant_t& asJson)
{
	try
	{
		// Получаем бинарные данные
		const std::vector<char>* dataPtr = nullptr;
		size_t dataSize = 0;

		if (std::holds_alternative<std::vector<char>>(avroData))
		{
			dataPtr = &std::get<std::vector<char>>(avroData);
			dataSize = dataPtr->size();
		}
		else if (std::holds_alternative<std::string>(avroData))
		{
			const std::string& str = std::get<std::string>(avroData);
			// Используем messageData для временного буфера
			messageData.assign(str.begin(), str.end());
			dataPtr = &messageData;
			dataSize = messageData.size();
		}
		else
		{
			msg_err = u8"Неверный тип данных для avroData. Ожидается строка или двоичные данные";
			return std::string("");
		}

		if (dataSize == 0)
		{
			msg_err = u8"AVRO данные пусты";
			return std::string("");
		}

		// Создаём поток для чтения из памяти
		std::unique_ptr<avro::InputStream> in = std::unique_ptr<avro::InputStream>(
			new MemoryInputStream(reinterpret_cast<const uint8_t*>(dataPtr->data()), dataSize));

		// Создаём DataFileReader
		avro::DataFileReader<avro::GenericDatum> reader(std::move(in));

		// Получаем схему из файла
		avro::ValidSchema schema = reader.dataSchema();

		// Проверяем, нужно ли использовать схему из карты
		std::string schemaName = std::get<std::string>(schemaJsonName);
		if (!schemaName.empty())
		{
			auto it = schemesMap.find(schemaName);
			if (it != schemesMap.end())
			{
				schema = it->second;
			}
		}

		bool returnAsJson = std::get<bool>(asJson);

		if (returnAsJson)
		{
			// Конвертируем в JSON
			boost::json::object resultJson;
			std::vector<avro::GenericDatum> records;

			// Читаем все записи
			avro::GenericDatum datum(schema);
			while (reader.read(datum))
			{
				records.push_back(datum);
			}

			if (records.empty())
			{
				return std::string("{}");
			}

			// Преобразуем в формат {"field1": [val1, val2, ...], "field2": [val1, val2, ...]}
			const avro::GenericRecord& firstRecord = records[0].value<avro::GenericRecord>();
			size_t fieldCount = firstRecord.fieldCount();

			// Получаем корень схемы для извлечения имён полей
			const avro::NodePtr& schemaNode = schema.root();

			for (size_t fieldIdx = 0; fieldIdx < fieldCount; ++fieldIdx)
			{
				// Получаем имя поля из схемы
				std::string fieldName = schemaNode->nameAt(fieldIdx);
				boost::json::array fieldValues;

				for (const auto& record : records)
				{
					const avro::GenericRecord& gr = record.value<avro::GenericRecord>();
					const avro::GenericDatum& fieldDatum = gr.fieldAt(fieldIdx);

					// Конвертируем значение в JSON
					boost::json::value jsonValue = convertAvroDatumToJson(fieldDatum);
					fieldValues.push_back(jsonValue);
				}

				resultJson[fieldName] = fieldValues;
			}

			return boost::json::serialize(resultJson);
		}
		else
		{
			// Возвращаем бинарные данные как есть
			return *dataPtr;
		}
	}
	catch (std::exception const& ex)
	{
		msg_err = "Error decoding AVRO: ";
		msg_err += ex.what();
		return std::string("");
	}
}

//================================== Protobuf ==========================================

// ProtobufContext class - encapsulates all protobuf-related objects
class SimpleKafka1C::ProtobufContext
{
public:
	std::map<std::string, const google::protobuf::Descriptor*> descriptors;
	google::protobuf::DescriptorPool pool;
	google::protobuf::DynamicMessageFactory factory;

	ProtobufContext() : factory(&pool) {}
};

bool SimpleKafka1C::putProtoSchema(const variant_t& schemaName, const variant_t& protoSchema)
{
	try
	{
		if (!protoContext)
		{
			protoContext = std::make_shared<ProtobufContext>();
		}

		std::string name = std::get<std::string>(schemaName);
		std::string schema = std::get<std::string>(protoSchema);

		// Check if schema already exists
		auto it = protoContext->descriptors.find(name);
		if (it != protoContext->descriptors.end())
		{
			// Schema already exists, skip
			return true;
		}

		// Parse the .proto schema
		google::protobuf::io::ArrayInputStream input(schema.data(), static_cast<int>(schema.size()));
		google::protobuf::io::Tokenizer tokenizer(&input, nullptr);

		google::protobuf::compiler::Parser parser;
		google::protobuf::FileDescriptorProto fileDescProto;
		fileDescProto.set_name(name + ".proto");

		if (!parser.Parse(&tokenizer, &fileDescProto))
		{
			msg_err = u8"Ошибка парсинга proto схемы";
			return false;
		}

		// Build descriptor from FileDescriptorProto
		const google::protobuf::FileDescriptor* fileDesc = protoContext->pool.BuildFile(fileDescProto);
		if (!fileDesc)
		{
			msg_err = u8"Не удалось построить дескриптор из proto схемы";
			return false;
		}

		// Find the message type (assuming first message in file)
		if (fileDesc->message_type_count() > 0)
		{
			const google::protobuf::Descriptor* descriptor = fileDesc->message_type(0);
			protoContext->descriptors[name] = descriptor;
		}
		else
		{
			msg_err = u8"Proto схема не содержит определений сообщений";
			return false;
		}
	}
	catch (std::exception const& ex)
	{
		msg_err = "Proto schema error: ";
		msg_err += ex.what();
		return false;
	}

	return msg_err.empty();
}

bool SimpleKafka1C::convertToProtobufFormat(const variant_t& msgJson, const variant_t& schemaName)
{
	protobufData.clear();

	try
	{
		if (!protoContext)
		{
			msg_err = u8"Protobuf контекст не инициализирован";
			return false;
		}

		std::string name = std::get<std::string>(schemaName);
		std::string jsonData = std::get<std::string>(msgJson);

		// Get descriptor
		auto it = protoContext->descriptors.find(name);
		if (it == protoContext->descriptors.end())
		{
			msg_err = u8"Схема protobuf не найдена: " + name;
			return false;
		}

		const google::protobuf::Descriptor* descriptor = it->second;

		// Create dynamic message
		const google::protobuf::Message* prototype = protoContext->factory.GetPrototype(descriptor);
		if (!prototype)
		{
			msg_err = u8"Не удалось создать прототип сообщения";
			return false;
		}

		std::unique_ptr<google::protobuf::Message> message(prototype->New());

		// Parse JSON to protobuf using boost::json
		try {
			boost::json::value json_val = boost::json::parse(jsonData);
			if (!json_val.is_object()) {
				msg_err = u8"JSON должен быть объектом";
				return false;
			}

			const boost::json::object& json_obj = json_val.as_object();
			for (auto& kv : json_obj) {
				const google::protobuf::FieldDescriptor* field =
					descriptor->FindFieldByName(std::string(kv.key()));
				if (field) {
					if (!SetProtobufFieldFromJson(message.get(), field, kv.value())) {
						msg_err = u8"Ошибка установки поля: " + std::string(kv.key());
						return false;
					}
				}
			}
		}
		catch (const std::exception& e) {
			msg_err = u8"Ошибка парсинга JSON: ";
			msg_err += e.what();
			return false;
		}

		// Serialize to binary
		if (!message->SerializeToString(&protobufData))
		{
			msg_err = u8"Ошибка сериализации protobuf сообщения";
			return false;
		}
	}
	catch (std::exception const& ex)
	{
		msg_err = "Protobuf conversion error: ";
		msg_err += ex.what();
		return false;
	}

	return msg_err.empty();
}

bool SimpleKafka1C::saveProtobufFile(const variant_t& fileName)
{
	if (protobufData.empty())
	{
		msg_err = u8"Protobuf данные пусты";
		return false;
	}

	try
	{
		std::ofstream out(std::get<std::string>(fileName), std::ios::out | std::ios::binary);
		out.write(protobufData.data(), protobufData.size());
		out.close();
	}
	catch (std::exception const& ex)
	{
		msg_err = ex.what();
		return false;
	}

	return msg_err.empty();
}

variant_t SimpleKafka1C::decodeProtobufMessage(const variant_t& protobufData, const variant_t& schemaName, const variant_t& asJson)
{
	try
	{
		if (!protoContext)
		{
			msg_err = u8"Protobuf контекст не инициализирован";
			return std::string("");
		}

		std::string name = std::get<std::string>(schemaName);

		// Get binary data
		const std::string* dataPtr = nullptr;
		std::string tempData;

		if (std::holds_alternative<std::vector<char>>(protobufData))
		{
			const std::vector<char>& vec = std::get<std::vector<char>>(protobufData);
			tempData = std::string(vec.begin(), vec.end());
			dataPtr = &tempData;
		}
		else if (std::holds_alternative<std::string>(protobufData))
		{
			dataPtr = &std::get<std::string>(protobufData);
		}
		else
		{
			msg_err = u8"Неверный тип данных для protobufData";
			return std::string("");
		}

		if (dataPtr->empty())
		{
			msg_err = u8"Protobuf данные пусты";
			return std::string("");
		}

		// Get descriptor
		auto it = protoContext->descriptors.find(name);
		if (it == protoContext->descriptors.end())
		{
			msg_err = u8"Схема protobuf не найдена: " + name;
			return std::string("");
		}

		const google::protobuf::Descriptor* descriptor = it->second;

		// Create dynamic message
		const google::protobuf::Message* prototype = protoContext->factory.GetPrototype(descriptor);
		if (!prototype)
		{
			msg_err = u8"Не удалось создать прототип сообщения";
			return std::string("");
		}

		std::unique_ptr<google::protobuf::Message> message(prototype->New());

		// Parse binary data
		if (!message->ParseFromString(*dataPtr))
		{
			msg_err = u8"Ошибка десериализации protobuf сообщения";
			return std::string("");
		}

		bool returnAsJson = std::get<bool>(asJson);

		if (returnAsJson)
		{
			// Convert to JSON using boost::json
			try {
				boost::json::object json_obj;

				for (int i = 0; i < descriptor->field_count(); i++)
				{
					const google::protobuf::FieldDescriptor* field = descriptor->field(i);
					const google::protobuf::Reflection* reflection = message->GetReflection();

					if (reflection->HasField(*message, field))
					{
						json_obj[field->name()] = GetJsonFromProtobufField(*message, field);
					}
				}

				std::string jsonOutput = boost::json::serialize(json_obj);
				return jsonOutput;
			}
			catch (const std::exception& e) {
				msg_err = u8"Ошибка преобразования protobuf в JSON: ";
				msg_err += e.what();
				return std::string("");
			}
		}
		else
		{
			// Return binary data as is
			return *dataPtr;
		}
	}
	catch (std::exception const& ex)
	{
		msg_err = "Protobuf decode error: ";
		msg_err += ex.what();
		return std::string("");
	}
}

int32_t SimpleKafka1C::produceProtobuf(const variant_t& topicName, const variant_t& partition, const variant_t& key, const variant_t& heads)
{
	cl_dr_cb.delivered = RdKafka::Message::MSG_STATUS_NOT_PERSISTED;
	if (hProducer == nullptr)
	{
		msg_err = u8"Продюсер не инициализирован";
		return -1;
	}
	if (protobufData.empty())
	{
		msg_err = u8"Protobuf данные пусты";
		return -1;
	}

	std::string tTopicName = std::get<std::string>(topicName);
	auto currentPartition = std::get<int>(partition);
	std::ofstream eventFile{};

	openEventFile(producerLogName, eventFile);
	if (eventFile.is_open())
	{
		eventFile << currentDateTime() << " Info: produceProtobuf. TopicName-" << tTopicName << " currentPartition-" << currentPartition << " protobufData.size()- " << protobufData.size() << std::endl;
	}

retry:
	RdKafka::ErrorCode resp = hProducer->produce(
		tTopicName,
		currentPartition == -1 ? RdKafka::Topic::PARTITION_UA : currentPartition,
		RdKafka::Producer::RK_MSG_COPY,
		const_cast<char*>(protobufData.data()),
		protobufData.size(),
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
		if (resp == RdKafka::ERR_MSG_SIZE_TOO_LARGE)
		{
			msg_err = u8"Размер сообщения превышает лимит message.max.bytes (по умолчанию 1 MB). Используйте УстановитьПараметр для увеличения лимита.";
			if (eventFile.is_open())
			{
				eventFile << currentDateTime() << " Error: " << msg_err << std::endl;
			}
			return cl_dr_cb.delivered;
		}
		msg_err = RdKafka::err2str(resp);
	}

	hProducer->poll(0 /*non-blocking*/);

	if (eventFile.is_open())
	{
		if (!msg_err.empty())
			eventFile << currentDateTime() << " Error produceProtobuf: " << msg_err << std::endl;
		else if (resp != RdKafka::ERR_NO_ERROR)
			eventFile << " Errorcode produceProtobuf: " << resp << std::endl;
		else
			eventFile << currentDateTime() << " Info produceProtobuf. Success" << std::endl;
	}

	return cl_dr_cb.delivered;
}

int32_t SimpleKafka1C::produceProtobufWithWaitResult(const variant_t& topicName, const variant_t& partition, const variant_t& key, const variant_t& heads)
{
	if (produceProtobuf(topicName, partition, key, heads) != -1)
	{
		hProducer->flush(20 * 1000);		 // wait for max 20 seconds
		if (hProducer->outq_len() > 0)
		{
			msg_err = u8"Не доставлено сообщений - " + std::to_string(hProducer->outq_len());

			std::ofstream eventFile{};
			openEventFile(producerLogName, eventFile);
			if (eventFile.is_open()) eventFile << currentDateTime() << " Info: produceProtobufWithWaitResult: " << msg_err << std::endl;

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
