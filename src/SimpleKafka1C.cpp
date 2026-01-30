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
#include <set>
#include <atomic>

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
	AddMethod(L"SetPartitioner", L"УстановитьПартишионер", this, &SimpleKafka1C::setPartitioner);
	AddMethod(L"InitializeProducer", L"ИнициализироватьПродюсера", this, &SimpleKafka1C::initProducer);
	AddMethod(L"Produce", L"ОтправитьСообщение", this, &SimpleKafka1C::produce,
		{ {2, -1}, {3, std::string("")}, {4, std::string("")} });
	AddMethod(L"ProduceWithWaitResult", L"ОтправитьСообщениеСОжиданиемРезультата", this, &SimpleKafka1C::produceWithWaitResult,
		{ {2, -1}, {3, std::string("")}, {4, std::string("")} });
	AddMethod(L"ProduceBatch", L"ОтправитьПакетСообщений", this, &SimpleKafka1C::produceBatch);
	AddMethod(L"StopProducer", L"ОстановитьПродюсера", this, &SimpleKafka1C::stopProducer);

	// + transactional producer
	AddMethod(L"InitTransactionalProducer", L"ИнициализироватьТранзакционногоПродюсера", this, &SimpleKafka1C::initTransactionalProducer);
	AddMethod(L"BeginTransaction", L"НачатьТранзакцию", this, &SimpleKafka1C::beginTransaction);
	AddMethod(L"CommitTransaction", L"ЗафиксироватьТранзакцию", this, &SimpleKafka1C::commitTransaction);
	AddMethod(L"AbortTransaction", L"ОтменитьТранзакцию", this, &SimpleKafka1C::abortTransaction);
	AddMethod(L"SendOffsetsToTransaction", L"ОтправитьОфсетыВТранзакцию", this, &SimpleKafka1C::sendOffsetsToTransaction);
	// - transactional producer

	AddMethod(L"InitializeConsumer", L"ИнициализироватьКонсьюмера", this, &SimpleKafka1C::initConsumer);
	AddMethod(L"Subscribe", L"Подписаться", this, &SimpleKafka1C::subscribe); // experimental
	AddMethod(L"Consume", L"Слушать", this, &SimpleKafka1C::consume);
	AddMethod(L"CommitOffset", L"ЗафиксироватьСмещение", this, &SimpleKafka1C::commitOffset, { {2, 0} });
	AddMethod(L"SetReadingPosition", L"УстановитьПозициюЧтения", this, &SimpleKafka1C::setReadingPosition, { {2, 0} });
	AddMethod(L"SetReadingPositions", L"УстановитьПозицииЧтения", this, &SimpleKafka1C::setReadingPositions); // experimental
	AddMethod(L"StopConsumer", L"ОстановитьКонсьюмера", this, &SimpleKafka1C::stopConsumer);
	AddMethod(L"SetWaitingTimeout", L"УстановитьТаймаутОжидания", this, &SimpleKafka1C::setWaitingTimeout);
	AddMethod(L"SetProducerFlushTimeout", L"УстановитьТаймаутОчисткиПродюсера", this, &SimpleKafka1C::setProducerFlushTimeout);
	AddMethod(L"SetConsumerCloseTimeout", L"УстановитьТаймаутОчисткиКонсьюмера", this, &SimpleKafka1C::setConsumerCloseTimeout);
	AddMethod(L"SetAdminOperationTimeout", L"УстановитьТаймаутАдминОпераций", this, &SimpleKafka1C::setAdminOperationTimeout);
	// + modern methods
	AddMethod(L"ReadMessage", L"ПрочитатьСообщение", this, &SimpleKafka1C::getMessage);
	AddMethod(L"ConsumeBatch", L"ЧитатьПакетСообщений", this, &SimpleKafka1C::consumeBatch,
		{ {0, 100}, {1, 1000} });
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

	// + cluster and broker information
	AddMethod(L"GetClusterInfo", L"ПолучитьИнформациюОКластере", this, &SimpleKafka1C::getClusterInfo);
	AddMethod(L"GetBrokerInfo", L"ПолучитьИнформациюОБрокере", this, &SimpleKafka1C::getBrokerInfo);
	AddMethod(L"GetPartitionWatermarks", L"ПолучитьГраницыПартиции", this, &SimpleKafka1C::getPartitionWatermarks);
	AddMethod(L"PingBroker", L"ПроверитьДоступностьБрокера", this, &SimpleKafka1C::pingBroker);
	AddMethod(L"GetPartitionMessageCount", L"ПолучитьКоличествоСообщенийВПартиции", this, &SimpleKafka1C::getPartitionMessageCount);
	AddMethod(L"GetBuiltinFeatures", L"ПолучитьВозможностиБиблиотеки", this, &SimpleKafka1C::getBuiltinFeatures);
	// - cluster and broker information

	// + consumer group management
	AddMethod(L"DeleteConsumerGroup", L"УдалитьГруппуКонсьюмеров", this, &SimpleKafka1C::deleteConsumerGroup);
	AddMethod(L"ResetConsumerGroupOffsets", L"СброситьОфсетыГруппыКонсьюмеров", this, &SimpleKafka1C::resetConsumerGroupOffsets);
	// - consumer group management

	// + advanced consumer position management
	AddMethod(L"SeekToBeginning", L"ПерейтиКНачалу", this, &SimpleKafka1C::seekToBeginning);
	AddMethod(L"SeekToEnd", L"ПерейтиККонцу", this, &SimpleKafka1C::seekToEnd);
	AddMethod(L"SeekToTimestamp", L"ПерейтиКВременнойМетке", this, &SimpleKafka1C::seekToTimestamp);
	// - advanced consumer position management

	// + consumer assignment (manual partition assignment)
	AddMethod(L"Assign", L"НазначитьПартиции", this, &SimpleKafka1C::assign);
	AddMethod(L"GetAssignment", L"ПолучитьНазначение", this, &SimpleKafka1C::getAssignment);
	AddMethod(L"Unassign", L"ОтменитьНазначение", this, &SimpleKafka1C::unassign);
	// - consumer assignment
	// - admin api

	AddMethod(L"Sleep", L"Пауза", this, &SimpleKafka1C::sleep);
	AddMethod(L"SetLogDirectory", L"УстановитьКаталогЛогов", this, &SimpleKafka1C::setLogDirectory);
	AddMethod(L"SetFormatLogFiles", L"УстановитьФорматЛогов", this, &SimpleKafka1C::setFormatLogFiles);

	// Metrics API
	AddMethod(L"GetProducerMetrics", L"ПолучитьМетрикиПродюсера", this, &SimpleKafka1C::getProducerMetrics);
	AddMethod(L"GetConsumerMetrics", L"ПолучитьМетрикиКонсьюмера", this, &SimpleKafka1C::getConsumerMetrics);
	AddMethod(L"ResetMetrics", L"СброситьМетрики", this, &SimpleKafka1C::resetMetrics);

	// AVRO
	AddMethod(L"PutAvroSchema", L"СохранитьСхемуAVRO", this, &SimpleKafka1C::putAvroSchema);
	AddMethod(L"ConvertToAvroFormat", L"ПреобразоватьВФорматAVRO", this, &SimpleKafka1C::convertToAvroFormat);
	AddMethod(L"ProduceAvro", L"ОтправитьСообщениеAVRO", this, &SimpleKafka1C::produceAvro,
		{ {1, -1}, {3, std::string("")}, {4, std::string("")} });
	AddMethod(L"ProduceAvroWithWaitResult", L"ОтправитьСообщениеAVROСОжиданиемРезультата", this, &SimpleKafka1C::produceAvroWithWaitResult,
		{ {1, -1}, {3, std::string("")}, {4, std::string("")} });
	AddMethod(L"SaveAvroFile", L"СохранитьФайлAVRO", this, &SimpleKafka1C::saveAvroFile);
	AddMethod(L"DecodeAvroMessage", L"ДекодироватьСообщениеAVRO", this, &SimpleKafka1C::decodeAvroMessage,
		{ {1, std::string("")}, {2, true} });

	// Protobuf
	AddMethod(L"PutProtoSchema", L"СохранитьСхемуProtobuf", this, &SimpleKafka1C::putProtoSchema);
	AddMethod(L"ConvertToProtobufFormat", L"ПреобразоватьВФорматProtobuf", this, &SimpleKafka1C::convertToProtobufFormat);
	AddMethod(L"SaveProtobufFile", L"СохранитьФайлProtobuf", this, &SimpleKafka1C::saveProtobufFile);
	AddMethod(L"DecodeProtobufMessage", L"ДекодироватьСообщениеProtobuf", this, &SimpleKafka1C::decodeProtobufMessage,
		{ {1, std::string("")}, {2, true} });
	AddMethod(L"ProduceProtobuf", L"ОтправитьСообщениеProtobuf", this, &SimpleKafka1C::produceProtobuf,
		{ {1, -1}, {3, std::string("")}, {4, std::string("")} });
	AddMethod(L"ProduceProtobufWithWaitResult", L"ОтправитьСообщениеProtobufСОжиданиемРезультата", this, &SimpleKafka1C::produceProtobufWithWaitResult,
		{ {1, -1}, {3, std::string("")}, {4, std::string("")} });

	// Schema Registry API
	AddMethod(L"RegisterSchema", L"ЗарегистрироватьСхему", this, &SimpleKafka1C::registerSchema);
	AddMethod(L"GetSchemaById", L"ПолучитьСхемуПоИД", this, &SimpleKafka1C::getSchemaById);
	AddMethod(L"GetLatestSchema", L"ПолучитьПоследнююСхему", this, &SimpleKafka1C::getLatestSchema);
	AddMethod(L"GetSchemaVersions", L"ПолучитьВерсииСхемы", this, &SimpleKafka1C::getSchemaVersions);
	AddMethod(L"DeleteSchema", L"УдалитьСхему", this, &SimpleKafka1C::deleteSchema);

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

	// Очистка CURL handle
	if (curlHandle)
	{
		curl_easy_cleanup(curlHandle);
		curlHandle = nullptr;
	}
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

bool SimpleKafka1C::setPartitioner(const variant_t& partitionerType)
{
	std::string type = std::get<std::string>(partitionerType);

	// Валидация типа партиционера
	static const std::set<std::string> validPartitioners = {
		"consistent",
		"consistent_random",
		"murmur2",
		"murmur2_random",
		"fnv1a",
		"fnv1a_random",
		"random"
	};

	if (validPartitioners.find(type) == validPartitioners.end())
	{
		msg_err = u8"Неизвестный тип партиционера: " + type +
			u8". Допустимые значения: consistent, consistent_random, murmur2, murmur2_random, fnv1a, fnv1a_random, random";
		return false;
	}

	partitionerStrategy = type;
	// Устанавливаем параметр через setParameter для применения при инициализации продюсера
	settings.push_back({ "partitioner", type });
	return true;
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

	RdKafka::ErrorCode resp;

	while (true)
	{
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

		if (resp == RdKafka::ERR__QUEUE_FULL)
		{
			hProducer->poll(1000 /*block for max 1000ms*/);
			if (eventFile.is_open())
			{
				eventFile << currentDateTime() << " Error: " << "Достигнуто максимальное количество ожидающих сообщений: queue.buffering.max.message" << std::endl;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(1000));
			continue;
		}
		break;
	}

	if (resp != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(resp);
		if (hdrs != nullptr)
		{
			delete hdrs;
		}
		cl_dr_cb.delivered = -1;
		producerMetrics.errorsCount++;
	}
	else
	{
		// Обновляем метрики при успешной отправке
		producerMetrics.messagesProduced++;
		if (std::holds_alternative<std::string>(msg))
		{
			producerMetrics.bytesProduced += std::get<std::string>(msg).size();
		}
		else
		{
			producerMetrics.bytesProduced += std::get<std::vector<char>>(msg).size();
		}
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

	RdKafka::ErrorCode resp;

	while (true)
	{
		resp = hProducer->produce(
			tTopicName,
			currentPartition == -1 ? RdKafka::Topic::PARTITION_UA : currentPartition,
			RdKafka::Producer::RK_MSG_COPY,
			avroFile.data(),
			avroFile.size(),
			std::get<std::string>(key).c_str(), std::get<std::string>(key).size(),
			0,
			nullptr,
			nullptr);

		if (resp == RdKafka::ERR__QUEUE_FULL)
		{
			hProducer->poll(1000 /*block for max 1000ms*/);
			if (eventFile.is_open())
			{
				eventFile << currentDateTime() << " Error: " << "Достигнуто максимальное количество ожидающих сообщений: queue.buffering.max.message" << std::endl;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(1000));
			continue;
		}
		break;
	}

	if (resp != RdKafka::ERR_NO_ERROR)
	{
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
		hProducer->flush(20 * 1000);		 // wait for max 20 seconds
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

int32_t SimpleKafka1C::produceBatch(const variant_t& messagesJson, const variant_t& topicName)
{
	if (hProducer == nullptr)
	{
		msg_err = u8"Продюсер не инициализирован";
		return -1;
	}

	std::string tTopicName = std::get<std::string>(topicName);
	std::string jsonStr = std::get<std::string>(messagesJson);

	std::ofstream eventFile{};
	openEventFile(producerLogName, eventFile);
	if (eventFile.is_open())
		eventFile << currentDateTime() << " Info: produceBatch. TopicName-" << tTopicName << std::endl;

	try
	{
		// Парсинг JSON массива
		boost::json::value jv = boost::json::parse(jsonStr);
		if (!jv.is_array())
		{
			msg_err = u8"JSON должен содержать массив сообщений";
			if (eventFile.is_open())
				eventFile << currentDateTime() << " Error: " << msg_err << std::endl;
			return -1;
		}

		boost::json::array messages = jv.as_array();
		int32_t successCount = 0;
		int32_t totalMessages = static_cast<int32_t>(messages.size());

		if (eventFile.is_open())
			eventFile << currentDateTime() << " Info: produceBatch. Processing " << totalMessages << " messages" << std::endl;

		// Отправка каждого сообщения
		for (const auto& msgObj : messages)
		{
			if (!msgObj.is_object())
			{
				if (eventFile.is_open())
					eventFile << currentDateTime() << " Warning: Skipping non-object element in array" << std::endl;
				continue;
			}

			const boost::json::object& msg = msgObj.as_object();

			// Извлекаем данные сообщения
			std::string message;
			std::string key;
			int32_t partition = -1;
			std::string headers;

			if (msg.contains("message"))
				message = boost::json::value_to<std::string>(msg.at("message"));
			else
			{
				if (eventFile.is_open())
					eventFile << currentDateTime() << " Warning: Message without 'message' field, skipping" << std::endl;
				continue;
			}

			if (msg.contains("key"))
				key = boost::json::value_to<std::string>(msg.at("key"));

			if (msg.contains("partition"))
				partition = static_cast<int32_t>(msg.at("partition").as_int64());

			if (msg.contains("headers"))
				headers = boost::json::value_to<std::string>(msg.at("headers"));

			// Подготовка headers
			RdKafka::Headers* hdrs = nullptr;
			if (!headers.empty())
			{
				std::vector<std::string> splitResult;
				boost::algorithm::split(splitResult, headers, boost::is_any_of(";"));
				hdrs = RdKafka::Headers::create();
				for (std::string& s : splitResult)
				{
					std::vector<std::string> hKeyValue;
					boost::algorithm::split(hKeyValue, s, boost::is_any_of(","));
					if (hKeyValue.size() == 2)
						hdrs->add(hKeyValue[0], hKeyValue[1]);
				}
			}

			// Отправка сообщения с retry логикой
			RdKafka::ErrorCode resp;

			while (true)
			{
				resp = hProducer->produce(
					tTopicName,
					partition == -1 ? RdKafka::Topic::PARTITION_UA : partition,
					RdKafka::Producer::RK_MSG_COPY,
					const_cast<char*>(message.c_str()), message.size(),
					key.c_str(), key.size(),
					0,
					hdrs,
					nullptr);

				if (resp == RdKafka::ERR__QUEUE_FULL)
				{
					hProducer->poll(1000);
					if (eventFile.is_open())
						eventFile << currentDateTime() << " Warning: Queue full, retrying..." << std::endl;
					std::this_thread::sleep_for(std::chrono::milliseconds(1000));
					continue;
				}
				break;
			}

			if (resp != RdKafka::ERR_NO_ERROR)
			{
				if (eventFile.is_open())
					eventFile << currentDateTime() << " Error: Failed to produce message: " << RdKafka::err2str(resp) << std::endl;

				if (hdrs != nullptr)
					delete hdrs;
			}
			else
			{
				successCount++;
			}

			hProducer->poll(0);
		}

		if (eventFile.is_open())
			eventFile << currentDateTime() << " Info: produceBatch. Successfully sent " << successCount << " of " << totalMessages << " messages" << std::endl;

		return successCount;
	}
	catch (const std::exception& e)
	{
		msg_err = std::string(u8"Ошибка при обработке JSON: ") + e.what();
		if (eventFile.is_open())
			eventFile << currentDateTime() << " Error: " << msg_err << std::endl;
		return -1;
	}
}

bool SimpleKafka1C::stopProducer()
{
	if (hProducer != nullptr)
	{
		RdKafka::ErrorCode flushResult = hProducer->flush(producerFlushTimeout);

		// Проверяем, остались ли недоставленные сообщения
		int outqLen = hProducer->outq_len();

		delete hProducer;
		hProducer = nullptr;

		// Если flush завершился по таймауту или остались сообщения в очереди
		if (flushResult == RdKafka::ERR__TIMED_OUT || outqLen > 0)
		{
			msg_err = "Producer flush timeout: " + std::to_string(outqLen) + " message(s) were not delivered";
			return false;
		}

		if (flushResult != RdKafka::ERR_NO_ERROR)
		{
			msg_err = "Producer flush error: " + RdKafka::err2str(flushResult);
			return false;
		}
	}
	return true;
}

//================================== Transactional Producer ==========================================

bool SimpleKafka1C::initTransactionalProducer(const variant_t& brokers, const variant_t& transactionalId)
{
	std::ofstream eventFile{};
	RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

	cl_dr_cb.logDir = std::get<std::string>(*logDirectory);
	cl_dr_cb.formatLogFiles = &*std::get<std::string>(*formatLogFiles).begin();
	cl_dr_cb.producerLogName = producerLogName;
	cl_dr_cb.pid = pid;
	cl_dr_cb.clientid = clientID();

	openEventFile(producerLogName, eventFile);
	if (eventFile.is_open()) eventFile << currentDateTime() << " Simple Kafka version: " << Version << " (librdkafka version: " << RdKafka::version_str() << ")" << std::endl;

	// Set transactional parameters
	if (conf->set("transactional.id", std::get<std::string>(transactionalId), msg_err) != RdKafka::Conf::CONF_OK)
	{
		if (eventFile.is_open()) eventFile << currentDateTime() << " Error setting transactional.id: " << msg_err << std::endl;
		return false;
	}

	// Enable idempotence (required for transactions)
	if (conf->set("enable.idempotence", "true", msg_err) != RdKafka::Conf::CONF_OK)
	{
		if (eventFile.is_open()) eventFile << currentDateTime() << " Error setting enable.idempotence: " << msg_err << std::endl;
		return false;
	}

	// Set additional parameters from settings
	for (size_t i = 0; i < settings.size(); i++)
	{
		if (conf->set(settings[i].Key, settings[i].Value, msg_err) != RdKafka::Conf::CONF_OK)
		{
			if (eventFile.is_open()) eventFile << currentDateTime() << " " << msg_err << std::endl;
			return false;
		}

		if (settings[i].Key == "statistics.interval.ms") cl_event_cb.statisticsOn = true;
	}

	// Set bootstrap servers
	if (conf->set("metadata.broker.list", std::get<std::string>(brokers), msg_err) != RdKafka::Conf::CONF_OK)
	{
		if (eventFile.is_open()) eventFile << currentDateTime() << " " << msg_err << std::endl;
		return false;
	}

	// Set callbacks
	conf->set("event_cb", &cl_event_cb, msg_err);
	conf->set("dr_cb", &cl_dr_cb, msg_err);

	// Create producer
	hProducer = RdKafka::Producer::create(conf, msg_err);

	if (!hProducer)
	{
		if (eventFile.is_open()) eventFile << currentDateTime() << " Failed to create producer: " << msg_err << std::endl;
		return false;
	}

	// Initialize transactions
	RdKafka::Error* error = hProducer->init_transactions(10000); // 10 second timeout

	if (error)
	{
		msg_err = error->str();
		if (eventFile.is_open()) eventFile << currentDateTime() << " Failed to initialize transactions: " << msg_err << std::endl;
		delete error;
		delete hProducer;
		hProducer = nullptr;
		return false;
	}

	if (eventFile.is_open()) eventFile << currentDateTime() << " Transactional producer initialized successfully with transactional.id: " << std::get<std::string>(transactionalId) << std::endl;

	delete conf;
	return true;
}

bool SimpleKafka1C::beginTransaction()
{
	if (!hProducer)
	{
		msg_err = "Producer not initialized. Call InitTransactionalProducer first.";
		return false;
	}

	RdKafka::Error* error = hProducer->begin_transaction();

	if (error)
	{
		msg_err = error->str();
		delete error;
		return false;
	}

	return true;
}

bool SimpleKafka1C::commitTransaction()
{
	if (!hProducer)
	{
		msg_err = "Producer not initialized. Call InitTransactionalProducer first.";
		return false;
	}

	RdKafka::Error* error = hProducer->commit_transaction(30000); // 30 second timeout

	if (error)
	{
		msg_err = error->str();
		delete error;
		return false;
	}

	return true;
}

bool SimpleKafka1C::abortTransaction()
{
	if (!hProducer)
	{
		msg_err = "Producer not initialized. Call InitTransactionalProducer first.";
		return false;
	}

	RdKafka::Error* error = hProducer->abort_transaction(30000); // 30 second timeout

	if (error)
	{
		msg_err = error->str();
		delete error;
		return false;
	}

	return true;
}

bool SimpleKafka1C::sendOffsetsToTransaction(const variant_t& offsetsJson, const variant_t& consumerGroupId)
{
	if (!hProducer)
	{
		msg_err = "Producer not initialized. Call InitTransactionalProducer first.";
		return false;
	}

	if (!hConsumer)
	{
		msg_err = "Consumer not initialized. Call InitializeConsumer first to use this method.";
		return false;
	}

	try
	{
		std::string jsonStr = std::get<std::string>(offsetsJson);
		std::string groupId = std::get<std::string>(consumerGroupId);

		// Parse JSON with offsets
		boost::json::value jv = boost::json::parse(jsonStr);
		boost::json::object obj = jv.as_object();

		if (!obj.contains("offsets"))
		{
			msg_err = "JSON must contain 'offsets' array";
			return false;
		}

		boost::json::array offsetsArray = obj["offsets"].as_array();
		std::vector<RdKafka::TopicPartition*> offsets;

		// Build offsets vector
		for (const auto& item : offsetsArray)
		{
			boost::json::object offsetObj = item.as_object();

			std::string topicName = offsetObj["topic"].as_string().c_str();
			int32_t partition = static_cast<int32_t>(offsetObj["partition"].as_int64());
			int64_t offset = offsetObj["offset"].as_int64();

			RdKafka::TopicPartition* tp = RdKafka::TopicPartition::create(topicName, partition, offset);
			offsets.push_back(tp);
		}

		// Commit offsets synchronously within the transaction context
		// This approach works with older librdkafka versions
		RdKafka::ErrorCode err = hConsumer->commitSync(offsets);

		// Cleanup
		for (auto* tp : offsets)
		{
			delete tp;
		}

		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = RdKafka::err2str(err);
			return false;
		}

		return true;
	}
	catch (const std::exception& e)
	{
		msg_err = std::string("Exception in sendOffsetsToTransaction: ") + e.what();
		return false;
	}
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

bool SimpleKafka1C::setProducerFlushTimeout(const variant_t& timeout)
{
	producerFlushTimeout = std::get<int32_t>(timeout);
	return true;
}

bool SimpleKafka1C::setConsumerCloseTimeout(const variant_t& timeout)
{
	consumerCloseTimeout = std::get<int32_t>(timeout);
	return true;
}

bool SimpleKafka1C::setAdminOperationTimeout(const variant_t& timeout)
{
	adminOperationTimeout = std::get<int32_t>(timeout);
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

		// Обновляем метрики консьюмера
		consumerMetrics.messagesConsumed++;
		consumerMetrics.bytesConsumed += this->messageLen;

		delete msg;
	}
	else
	{
		if (resultConsume == RdKafka::ERR__TIMED_OUT) {
			consumerMetrics.pollTimeouts++;
		}
		else {
			msg_err = msg->errstr();
			consumerMetrics.errorsCount++;
			if (eventFile.is_open()) eventFile << currentDateTime() << " Error: " << msg_err << std::endl;
		}
		delete msg;
		return false;
	}

	return true;
}

std::string SimpleKafka1C::consumeBatch(const variant_t& maxMessages, const variant_t& maxWaitMs)
{
	if (hConsumer == nullptr)
	{
		msg_err = u8"Консьюмер не инициализирован";
		return "";
	}

	int32_t maxMsgCount = std::get<int32_t>(maxMessages);
	int32_t maxWait = std::get<int32_t>(maxWaitMs);

	if (maxMsgCount <= 0) maxMsgCount = 100;
	if (maxWait <= 0) maxWait = 1000;

	boost::json::array messagesArray;
	auto startTime = std::chrono::steady_clock::now();
	int32_t messagesRead = 0;

	while (messagesRead < maxMsgCount)
	{
		// Проверяем, не истекло ли время ожидания
		auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::steady_clock::now() - startTime).count();

		if (elapsed >= maxWait)
			break;

		int32_t remainingWait = static_cast<int32_t>(maxWait - elapsed);
		if (remainingWait <= 0) remainingWait = 1;

		RdKafka::Message* msg = hConsumer->consume(remainingWait);
		RdKafka::ErrorCode resultConsume = msg->err();

		if (resultConsume == RdKafka::ERR_NO_ERROR)
		{
			boost::json::object msgObj;

			// Данные сообщения
			char* payload = static_cast<char*>(msg->payload());
			msgObj["message"] = std::string(payload, msg->len());

			// Ключ
			if (msg->key() && !msg->key()->empty())
			{
				msgObj["key"] = *msg->key();
			}

			// Метаданные
			msgObj["topic"] = msg->topic_name();
			msgObj["partition"] = msg->partition();
			msgObj["offset"] = msg->offset();
			msgObj["broker_id"] = msg->broker_id();

			RdKafka::MessageTimestamp ts = msg->timestamp();
			msgObj["timestamp"] = ts.timestamp;

			// Заголовки
			RdKafka::Headers* headers = msg->headers();
			if (headers)
			{
				boost::json::array headersArray;
				std::vector<RdKafka::Headers::Header> hdrs = headers->get_all();
				for (const auto& hdr : hdrs)
				{
					boost::json::object headerObj;
					headerObj["key"] = hdr.key();
					headerObj["value"] = std::string(static_cast<const char*>(hdr.value()), hdr.value_size());
					headersArray.push_back(headerObj);
				}
				msgObj["headers"] = headersArray;
			}

			messagesArray.push_back(msgObj);
			messagesRead++;

			// Обновляем метрики
			consumerMetrics.messagesConsumed++;
			consumerMetrics.bytesConsumed += msg->len();
		}
		else if (resultConsume == RdKafka::ERR__TIMED_OUT)
		{
			consumerMetrics.pollTimeouts++;
			// При таймауте выходим из цикла, если уже есть сообщения
			if (messagesRead > 0)
			{
				delete msg;
				break;
			}
		}
		else
		{
			consumerMetrics.errorsCount++;
		}

		delete msg;
	}

	boost::json::object result;
	result["messages"] = messagesArray;
	result["count"] = messagesRead;

	return boost::json::serialize(result);
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

double SimpleKafka1C::getMessageTimestamp()
{
	return static_cast<double>(this->timestamp) / 1000.0;
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

	RdKafka::ErrorCode err = hConsumer->commitSync(offsets);

	// Очистка TopicPartition* для предотвращения утечки памяти
	for (auto tp : offsets) {
		delete tp;
	}

	if (err != RdKafka::ERR_NO_ERROR)
	{
		return false;
	}
	return true;
}

bool SimpleKafka1C::stopConsumer()
{
	if (hConsumer != nullptr)
	{
		RdKafka::ErrorCode closeErr = hConsumer->close();

		if (closeErr != RdKafka::ERR_NO_ERROR)
		{
			msg_err = "Consumer close error: " + RdKafka::err2str(closeErr);
			delete hConsumer;
			hConsumer = nullptr;
			RdKafka::wait_destroyed(consumerCloseTimeout);
			return false;
		}

		delete hConsumer;
		hConsumer = nullptr;

		// Ожидаем завершения всех фоновых операций
		int waitResult = RdKafka::wait_destroyed(consumerCloseTimeout);
		if (waitResult != 0)
		{
			msg_err = "Consumer wait_destroyed timeout: " + std::to_string(waitResult) + " object(s) still exist";
			return false;
		}
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

//================================== AdminClientScope =========================================

SimpleKafka1C::AdminClientScope::AdminClientScope(const std::string& brokers,
                                                   const std::vector<KafkaSettings>& settings,
                                                   rd_kafka_type_t type)
{
	char errstr[512];

	rd_kafka_conf_t* conf = rd_kafka_conf_new();

	// Применяем дополнительные настройки
	for (const auto& setting : settings)
	{
		if (rd_kafka_conf_set(conf, setting.Key.c_str(), setting.Value.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			errstr_msg = errstr;
			rd_kafka_conf_destroy(conf);
			return;
		}
	}

	// Устанавливаем bootstrap.servers
	if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		errstr_msg = errstr;
		rd_kafka_conf_destroy(conf);
		return;
	}

	// Создаем клиента
	rk = rd_kafka_new(type, conf, errstr, sizeof(errstr));
	if (!rk)
	{
		errstr_msg = std::string(u8"Ошибка создания клиента: ") + errstr;
		return;
	}

	// Создаем очередь для получения результатов
	rkqu = rd_kafka_queue_new(rk);
}

SimpleKafka1C::AdminClientScope::~AdminClientScope()
{
	if (rkqu)
	{
		rd_kafka_queue_destroy(rkqu);
	}
	if (rk)
	{
		rd_kafka_destroy(rk);
	}
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

	// запрещаем автосоздание топиков при запросе метаданных
	if (conf->set("allow.auto.create.topics", "false", msg_err) != RdKafka::Conf::CONF_OK)
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

//================================== Cluster and Broker Information ==========================================

std::string SimpleKafka1C::getClusterInfo(const variant_t& brokers)
{
	std::string result;
	std::stringstream s{};

	std::string tBrokers = std::get<std::string>(brokers);

	// создаем конфигурацию
	RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

	// дополнительные параметры
	for (size_t i = 0; i < settings.size(); i++)
	{
		if (conf->set(settings[i].Key, settings[i].Value, result) != RdKafka::Conf::CONF_OK)
		{
			msg_err = result;
			delete conf;
			return "";
		}
	}

	if (conf->set("bootstrap.servers", tBrokers, result) != RdKafka::Conf::CONF_OK)
	{
		msg_err = result;
		delete conf;
		return "";
	}

	// создаем продюсера для получения метаданных
	RdKafka::Producer* producer = RdKafka::Producer::create(conf, result);
	if (!producer)
	{
		msg_err = u8"Ошибка создания клиента: " + result;
		delete conf;
		return "";
	}

	// получаем метаданные кластера
	RdKafka::Metadata* metadata = nullptr;
	RdKafka::ErrorCode err = producer->metadata(true, nullptr, &metadata, 5000);

	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(err);
		delete producer;
		delete conf;
		return "";
	}

	// формируем JSON результат
	boost::property_tree::ptree jsonObj;
	boost::property_tree::ptree brokersChildren;

	jsonObj.put("cluster_id", metadata->orig_broker_id());
	jsonObj.put("brokers_count", metadata->brokers()->size());
	jsonObj.put("topics_count", metadata->topics()->size());

	// информация о брокерах
	const RdKafka::Metadata::BrokerMetadataVector* brokers_vec = metadata->brokers();
	for (auto it = brokers_vec->begin(); it != brokers_vec->end(); ++it)
	{
		boost::property_tree::ptree brokerInfo;
		brokerInfo.put("id", std::to_string((*it)->id()));
		brokerInfo.put("host", (*it)->host());
		brokerInfo.put("port", std::to_string((*it)->port()));
		brokersChildren.push_back(std::make_pair("", brokerInfo));
	}

	jsonObj.put_child("brokers", brokersChildren);

	delete metadata;
	delete producer;
	delete conf;

	boost::property_tree::write_json(s, jsonObj, true);
	return s.str();
}

std::string SimpleKafka1C::getBrokerInfo(const variant_t& brokers, const variant_t& brokerId)
{
	std::string result;
	std::stringstream s{};

	std::string tBrokers = std::get<std::string>(brokers);
	int32_t tBrokerId = std::get<int32_t>(brokerId);

	// создаем конфигурацию
	RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

	// дополнительные параметры
	for (size_t i = 0; i < settings.size(); i++)
	{
		if (conf->set(settings[i].Key, settings[i].Value, result) != RdKafka::Conf::CONF_OK)
		{
			msg_err = result;
			delete conf;
			return "";
		}
	}

	if (conf->set("bootstrap.servers", tBrokers, result) != RdKafka::Conf::CONF_OK)
	{
		msg_err = result;
		delete conf;
		return "";
	}

	// создаем продюсера для получения метаданных
	RdKafka::Producer* producer = RdKafka::Producer::create(conf, result);
	if (!producer)
	{
		msg_err = u8"Ошибка создания клиента: " + result;
		delete conf;
		return "";
	}

	// получаем метаданные кластера
	RdKafka::Metadata* metadata = nullptr;
	RdKafka::ErrorCode err = producer->metadata(true, nullptr, &metadata, 5000);

	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(err);
		delete producer;
		delete conf;
		return "";
	}

	// ищем брокера по ID
	const RdKafka::Metadata::BrokerMetadataVector* brokers_vec = metadata->brokers();
	bool found = false;
	boost::property_tree::ptree jsonObj;

	for (auto it = brokers_vec->begin(); it != brokers_vec->end(); ++it)
	{
		if ((*it)->id() == tBrokerId)
		{
			jsonObj.put("id", std::to_string((*it)->id()));
			jsonObj.put("host", (*it)->host());
			jsonObj.put("port", std::to_string((*it)->port()));

			// подсчитываем количество партиций на этом брокере
			int leader_partitions = 0;
			int replica_partitions = 0;

			const RdKafka::Metadata::TopicMetadataVector* topics = metadata->topics();
			for (auto topic_it = topics->begin(); topic_it != topics->end(); ++topic_it)
			{
				const std::vector<const RdKafka::PartitionMetadata*>* partitions = (*topic_it)->partitions();
				for (auto part_it = partitions->begin(); part_it != partitions->end(); ++part_it)
				{
					if ((*part_it)->leader() == tBrokerId)
					{
						leader_partitions++;
					}

					const std::vector<int32_t>* replicas = (*part_it)->replicas();
					for (auto replica : *replicas)
					{
						if (replica == tBrokerId)
						{
							replica_partitions++;
							break;
						}
					}
				}
			}

			jsonObj.put("leader_partitions_count", leader_partitions);
			jsonObj.put("replica_partitions_count", replica_partitions);

			found = true;
			break;
		}
	}

	delete metadata;
	delete producer;
	delete conf;

	if (!found)
	{
		msg_err = u8"Брокер с ID " + std::to_string(tBrokerId) + u8" не найден";
		return "";
	}

	boost::property_tree::write_json(s, jsonObj, true);
	return s.str();
}

std::string SimpleKafka1C::getPartitionWatermarks(const variant_t& brokers,
                                                   const variant_t& topicName,
                                                   const variant_t& partition)
{
	std::string result;
	std::stringstream s{};

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);
	int32_t tPartition = std::get<int32_t>(partition);

	// создаем конфигурацию
	RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

	// дополнительные параметры
	for (size_t i = 0; i < settings.size(); i++)
	{
		if (conf->set(settings[i].Key, settings[i].Value, result) != RdKafka::Conf::CONF_OK)
		{
			msg_err = result;
			delete conf;
			return "";
		}
	}

	if (conf->set("bootstrap.servers", tBrokers, result) != RdKafka::Conf::CONF_OK)
	{
		msg_err = result;
		delete conf;
		return "";
	}

	// запрещаем автосоздание топиков
	if (conf->set("allow.auto.create.topics", "false", result) != RdKafka::Conf::CONF_OK)
	{
		msg_err = result;
		delete conf;
		return "";
	}

	// создаем продюсера для получения watermarks
	RdKafka::Producer* producer = RdKafka::Producer::create(conf, result);
	if (!producer)
	{
		msg_err = u8"Ошибка создания клиента: " + result;
		delete conf;
		return "";
	}

	// получаем watermarks для партиции
	int64_t low = 0;
	int64_t high = 0;
	RdKafka::ErrorCode err = producer->query_watermark_offsets(tTopicName, tPartition, &low, &high, 5000);

	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(err);
		delete producer;
		delete conf;
		return "";
	}

	// формируем JSON результат
	boost::property_tree::ptree jsonObj;
	jsonObj.put("topic", tTopicName);
	jsonObj.put("partition", std::to_string(tPartition));
	jsonObj.put("low_watermark", std::to_string(low));
	jsonObj.put("high_watermark", std::to_string(high));
	jsonObj.put("message_count", std::to_string(high - low));

	delete producer;
	delete conf;

	boost::property_tree::write_json(s, jsonObj, true);
	return s.str();
}

bool SimpleKafka1C::pingBroker(const variant_t& brokers, const variant_t& timeout)
{
	std::string result;
	std::string tBrokers = std::get<std::string>(brokers);
	int32_t tTimeout = std::get<int32_t>(timeout);

	// создаем конфигурацию
	RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

	// дополнительные параметры
	for (size_t i = 0; i < settings.size(); i++)
	{
		if (conf->set(settings[i].Key, settings[i].Value, result) != RdKafka::Conf::CONF_OK)
		{
			msg_err = result;
			delete conf;
			return false;
		}
	}

	if (conf->set("bootstrap.servers", tBrokers, result) != RdKafka::Conf::CONF_OK)
	{
		msg_err = result;
		delete conf;
		return false;
	}

	// создаем продюсера для проверки подключения
	RdKafka::Producer* producer = RdKafka::Producer::create(conf, result);
	if (!producer)
	{
		msg_err = u8"Ошибка подключения к брокеру: " + result;
		delete conf;
		return false;
	}

	// Пытаемся получить метаданные - это проверит доступность брокера
	RdKafka::Metadata* metadata = nullptr;
	RdKafka::ErrorCode err = producer->metadata(true, nullptr, &metadata, tTimeout);

	delete producer;
	delete conf;

	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = u8"Брокер недоступен: " + std::string(RdKafka::err2str(err));
		if (metadata)
			delete metadata;
		return false;
	}

	if (metadata)
		delete metadata;

	return true;
}

double SimpleKafka1C::getPartitionMessageCount(const variant_t& brokers,
                                                 const variant_t& topicName,
                                                 const variant_t& partition)
{
	std::string result;
	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);
	int32_t tPartition = std::get<int32_t>(partition);

	// создаем конфигурацию
	RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

	// дополнительные параметры
	for (size_t i = 0; i < settings.size(); i++)
	{
		if (conf->set(settings[i].Key, settings[i].Value, result) != RdKafka::Conf::CONF_OK)
		{
			msg_err = result;
			delete conf;
			return -1.0;
		}
	}

	if (conf->set("bootstrap.servers", tBrokers, result) != RdKafka::Conf::CONF_OK)
	{
		msg_err = result;
		delete conf;
		return -1.0;
	}

	// запрещаем автосоздание топиков
	if (conf->set("allow.auto.create.topics", "false", result) != RdKafka::Conf::CONF_OK)
	{
		msg_err = result;
		delete conf;
		return -1.0;
	}

	// создаем продюсера для получения watermarks
	RdKafka::Producer* producer = RdKafka::Producer::create(conf, result);
	if (!producer)
	{
		msg_err = u8"Ошибка создания клиента: " + result;
		delete conf;
		return -1.0;
	}

	// получаем watermarks для партиции
	int64_t low = 0;
	int64_t high = 0;
	RdKafka::ErrorCode err = producer->query_watermark_offsets(tTopicName, tPartition, &low, &high, 5000);

	delete producer;
	delete conf;

	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(err);
		return -1.0;
	}

	// Возвращаем количество сообщений (high - low)
	return static_cast<double>(high - low);
}

std::string SimpleKafka1C::getBuiltinFeatures()
{
	std::stringstream s{};
	boost::property_tree::ptree jsonObj;

	// Версия librdkafka
	jsonObj.put("version", RdKafka::version_str());

	// Получаем builtin.features через конфигурацию
	RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	std::string features;

	if (conf->get("builtin.features", features) == RdKafka::Conf::CONF_OK)
	{
		jsonObj.put("builtin_features", features);

		// Разбираем на отдельные флаги для удобства
		boost::property_tree::ptree featuresArray;
		std::istringstream iss(features);
		std::string feature;
		while (std::getline(iss, feature, ','))
		{
			boost::property_tree::ptree node;
			node.put("", feature);
			featuresArray.push_back(boost::property_tree::ptree::value_type("", node));
		}
		if (featuresArray.size())
		{
			jsonObj.put_child("features", featuresArray);
		}
	}

	delete conf;

	boost::property_tree::write_json(s, jsonObj, true);
	return s.str();
}

//================================== Consumer Group Management ==========================================

bool SimpleKafka1C::deleteConsumerGroup(const variant_t& brokers, const variant_t& groupId)
{
	char errstr[512];
	std::string tBrokers = std::get<std::string>(brokers);
	std::string tGroupId = std::get<std::string>(groupId);

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

	// создаем клиента для Admin API
	rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!rk)
	{
		msg_err = u8"Ошибка создания клиента: " + std::string(errstr);
		return false;
	}

	// создаем очередь для получения результатов
	rd_kafka_queue_t* rkqu = rd_kafka_queue_new(rk);

	// создаем список групп для удаления
	const char* group_array[1] = { tGroupId.c_str() };
	rd_kafka_DeleteConsumerGroupOffsets_t* del_groups[1];

	// Используем DeleteConsumerGroupOffsets для удаления офсетов группы
	// Если нужно удалить саму группу, она должна быть неактивной (без потребителей)
	del_groups[0] = rd_kafka_DeleteConsumerGroupOffsets_new(tGroupId.c_str(), NULL);

	// опции для операции
	rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DELETECONSUMERGROUPOFFSETS);
	rd_kafka_AdminOptions_set_request_timeout(options, 10000, errstr, sizeof(errstr));

	// выполняем операцию удаления
	rd_kafka_DeleteConsumerGroupOffsets(rk, del_groups, 1, options, rkqu);

	// ожидаем результат
	rd_kafka_event_t* rkev = rd_kafka_queue_poll(rkqu, 12000);

	// освобождаем ресурсы
	rd_kafka_DeleteConsumerGroupOffsets_destroy(del_groups[0]);
	rd_kafka_AdminOptions_destroy(options);

	if (!rkev)
	{
		msg_err = u8"Таймаут при удалении группы консьюмеров";
		rd_kafka_queue_destroy(rkqu);
		rd_kafka_destroy(rk);
		return false;
	}

	if (rd_kafka_event_error(rkev))
	{
		msg_err = rd_kafka_event_error_string(rkev);
		rd_kafka_event_destroy(rkev);
		rd_kafka_queue_destroy(rkqu);
		rd_kafka_destroy(rk);
		return false;
	}

	rd_kafka_event_destroy(rkev);
	rd_kafka_queue_destroy(rkqu);
	rd_kafka_destroy(rk);

	return true;
}

bool SimpleKafka1C::resetConsumerGroupOffsets(const variant_t& brokers, const variant_t& groupId,
                                               const variant_t& topicName, const variant_t& resetTo)
{
	if (!hConsumer)
	{
		msg_err = "Consumer not initialized. Call InitializeConsumer first.";
		return false;
	}

	try
	{
		std::string tTopicName = std::get<std::string>(topicName);
		std::string tResetTo = std::get<std::string>(resetTo);

		// Получаем метаданные топика для определения количества партиций
		RdKafka::Metadata* metadata = nullptr;
		RdKafka::ErrorCode err = hConsumer->metadata(false, nullptr, &metadata, 5000);

		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = RdKafka::err2str(err);
			return false;
		}

		// Находим топик в метаданных
		const RdKafka::Metadata::TopicMetadataVector* topics = metadata->topics();
		int partition_cnt = 0;

		for (auto it = topics->begin(); it != topics->end(); ++it)
		{
			if ((*it)->topic() == tTopicName)
			{
				partition_cnt = (*it)->partitions()->size();
				break;
			}
		}

		delete metadata;

		if (partition_cnt == 0)
		{
			msg_err = "Topic not found or has no partitions";
			return false;
		}

		// Создаем список партиций для сброса офсетов
		std::vector<RdKafka::TopicPartition*> partitions;

		for (int i = 0; i < partition_cnt; i++)
		{
			int64_t offset;

			if (tResetTo == "earliest")
			{
				offset = RdKafka::Topic::OFFSET_BEGINNING;
			}
			else if (tResetTo == "latest")
			{
				offset = RdKafka::Topic::OFFSET_END;
			}
			else
			{
				// Пытаемся преобразовать в timestamp
				try
				{
					offset = std::stoll(tResetTo);
				}
				catch (...)
				{
					msg_err = "Invalid resetTo value. Use 'earliest', 'latest', or timestamp";
					return false;
				}
			}

			RdKafka::TopicPartition* tp = RdKafka::TopicPartition::create(tTopicName, i, offset);
			partitions.push_back(tp);
		}

		// Фиксируем офсеты
		err = hConsumer->commitSync(partitions);

		// Освобождаем память
		for (auto* tp : partitions)
		{
			delete tp;
		}

		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = RdKafka::err2str(err);
			return false;
		}

		return true;
	}
	catch (const std::exception& e)
	{
		msg_err = std::string("Exception in resetConsumerGroupOffsets: ") + e.what();
		return false;
	}
}

//================================== Advanced Consumer Position Management ==========================================

bool SimpleKafka1C::seekToBeginning(const variant_t& topicName, const variant_t& partition)
{
	if (!hConsumer)
	{
		msg_err = "Consumer not initialized. Call InitializeConsumer first.";
		return false;
	}

	try
	{
		std::string tTopicName = std::get<std::string>(topicName);
		int32_t tPartition = std::get<int32_t>(partition);

		// Создаем TopicPartition с offset BEGINNING
		RdKafka::TopicPartition* tp = RdKafka::TopicPartition::create(tTopicName, tPartition, RdKafka::Topic::OFFSET_BEGINNING);
		std::vector<RdKafka::TopicPartition*> partitions;
		partitions.push_back(tp);

		// Выполняем seek
		RdKafka::ErrorCode err = hConsumer->seek(*tp, 5000);

		delete tp;

		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = RdKafka::err2str(err);
			return false;
		}

		return true;
	}
	catch (const std::exception& e)
	{
		msg_err = std::string("Exception in seekToBeginning: ") + e.what();
		return false;
	}
}

bool SimpleKafka1C::seekToEnd(const variant_t& topicName, const variant_t& partition)
{
	if (!hConsumer)
	{
		msg_err = "Consumer not initialized. Call InitializeConsumer first.";
		return false;
	}

	try
	{
		std::string tTopicName = std::get<std::string>(topicName);
		int32_t tPartition = std::get<int32_t>(partition);

		// Создаем TopicPartition с offset END
		RdKafka::TopicPartition* tp = RdKafka::TopicPartition::create(tTopicName, tPartition, RdKafka::Topic::OFFSET_END);

		// Выполняем seek
		RdKafka::ErrorCode err = hConsumer->seek(*tp, 5000);

		delete tp;

		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = RdKafka::err2str(err);
			return false;
		}

		return true;
	}
	catch (const std::exception& e)
	{
		msg_err = std::string("Exception in seekToEnd: ") + e.what();
		return false;
	}
}

bool SimpleKafka1C::seekToTimestamp(const variant_t& topicName, const variant_t& partition, const variant_t& timestamp)
{
	if (!hConsumer)
	{
		msg_err = "Consumer not initialized. Call InitializeConsumer first.";
		return false;
	}

	try
	{
		std::string tTopicName = std::get<std::string>(topicName);
		int32_t tPartition = std::get<int32_t>(partition);
		int64_t tTimestamp = std::get<int32_t>(timestamp); // timestamp в миллисекундах

		// Создаем TopicPartition для поиска по timestamp
		RdKafka::TopicPartition* tp = RdKafka::TopicPartition::create(tTopicName, tPartition);
		std::vector<RdKafka::TopicPartition*> partitions;
		partitions.push_back(tp);

		// Устанавливаем timestamp для поиска
		tp->set_offset(tTimestamp);

		// Получаем офсет для указанного timestamp
		RdKafka::ErrorCode err = hConsumer->offsetsForTimes(partitions, 5000);

		if (err != RdKafka::ERR_NO_ERROR)
		{
			delete tp;
			msg_err = RdKafka::err2str(err);
			return false;
		}

		// Получаем найденный офсет
		int64_t foundOffset = tp->offset();

		if (foundOffset < 0)
		{
			delete tp;
			msg_err = "No offset found for the specified timestamp";
			return false;
		}

		// Выполняем seek к найденному офсету
		err = hConsumer->seek(*tp, 5000);

		delete tp;

		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = RdKafka::err2str(err);
			return false;
		}

		return true;
	}
	catch (const std::exception& e)
	{
		msg_err = std::string("Exception in seekToTimestamp: ") + e.what();
		return false;
	}
}

//================================== Consumer Assignment (Manual Partition Assignment) ==========================================

bool SimpleKafka1C::assign(const variant_t& jsonTopicPartitions)
{
	if (!hConsumer)
	{
		msg_err = "Consumer not initialized. Call InitializeConsumer first.";
		return false;
	}

	try
	{
		std::string jsonStr = std::get<std::string>(jsonTopicPartitions);
		boost::json::value jv = boost::json::parse(jsonStr);

		if (!jv.is_array())
		{
			msg_err = u8"JSON должен содержать массив топиков и партиций";
			return false;
		}

		boost::json::array topicPartitions = jv.as_array();
		std::vector<RdKafka::TopicPartition*> partitions;

		for (const auto& item : topicPartitions)
		{
			if (!item.is_object())
				continue;

			const boost::json::object& obj = item.as_object();

			if (!obj.contains("topic") || !obj.contains("partition"))
			{
				msg_err = u8"Каждый элемент должен содержать 'topic' и 'partition'";
				for (auto* tp : partitions)
					delete tp;
				return false;
			}

			std::string topic = boost::json::value_to<std::string>(obj.at("topic"));
			int32_t partition = static_cast<int32_t>(obj.at("partition").as_int64());

			// Опционально: можно задать начальный offset
			int64_t offset = RdKafka::Topic::OFFSET_INVALID;
			if (obj.contains("offset"))
			{
				offset = obj.at("offset").as_int64();
			}

			RdKafka::TopicPartition* tp = RdKafka::TopicPartition::create(topic, partition, offset);
			partitions.push_back(tp);
		}

		if (partitions.empty())
		{
			msg_err = u8"Не найдено валидных топиков и партиций для назначения";
			return false;
		}

		// Выполняем assign
		RdKafka::ErrorCode err = hConsumer->assign(partitions);

		// Освобождаем память
		for (auto* tp : partitions)
			delete tp;

		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = RdKafka::err2str(err);
			return false;
		}

		return true;
	}
	catch (const std::exception& e)
	{
		msg_err = std::string("Exception in assign: ") + e.what();
		return false;
	}
}

std::string SimpleKafka1C::getAssignment()
{
	if (!hConsumer)
	{
		msg_err = "Consumer not initialized. Call InitializeConsumer first.";
		return "";
	}

	try
	{
		std::vector<RdKafka::TopicPartition*> partitions;
		RdKafka::ErrorCode err = hConsumer->assignment(partitions);

		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = RdKafka::err2str(err);
			return "";
		}

		// Формируем JSON с назначенными партициями
		std::stringstream s{};
		boost::property_tree::ptree jsonObj;
		boost::property_tree::ptree partitionsArray;

		jsonObj.put("count", partitions.size());

		for (const auto* tp : partitions)
		{
			boost::property_tree::ptree partitionObj;
			partitionObj.put("topic", tp->topic());
			partitionObj.put("partition", tp->partition());
			partitionObj.put("offset", tp->offset());
			partitionsArray.push_back(std::make_pair("", partitionObj));
		}

		jsonObj.put_child("partitions", partitionsArray);

		// Освобождаем память
		for (auto* tp : partitions)
			delete tp;

		boost::property_tree::write_json(s, jsonObj, true);
		return s.str();
	}
	catch (const std::exception& e)
	{
		msg_err = std::string("Exception in getAssignment: ") + e.what();
		return "";
	}
}

bool SimpleKafka1C::unassign()
{
	if (!hConsumer)
	{
		msg_err = "Consumer not initialized. Call InitializeConsumer first.";
		return false;
	}

	try
	{
		// Передаем пустой список партиций для отмены назначения
		std::vector<RdKafka::TopicPartition*> empty;
		RdKafka::ErrorCode err = hConsumer->assign(empty);

		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = RdKafka::err2str(err);
			return false;
		}

		return true;
	}
	catch (const std::exception& e)
	{
		msg_err = std::string("Exception in unassign: ") + e.what();
		return false;
	}
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

//================================== Metrics API ==========================================

std::string SimpleKafka1C::getProducerMetrics()
{
	boost::json::object result;

	result["messages_produced"] = producerMetrics.messagesProduced.load();
	result["bytes_produced"] = producerMetrics.bytesProduced.load();
	result["errors_count"] = producerMetrics.errorsCount.load();
	result["retries_count"] = producerMetrics.retriesCount.load();

	// Вычисляем время работы в секундах
	auto now = std::chrono::steady_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - producerMetrics.startTime).count();
	result["uptime_seconds"] = duration;

	// Средняя скорость сообщений в секунду
	if (duration > 0)
	{
		result["messages_per_second"] = static_cast<double>(producerMetrics.messagesProduced.load()) / duration;
		result["bytes_per_second"] = static_cast<double>(producerMetrics.bytesProduced.load()) / duration;
	}
	else
	{
		result["messages_per_second"] = 0.0;
		result["bytes_per_second"] = 0.0;
	}

	return boost::json::serialize(result);
}

std::string SimpleKafka1C::getConsumerMetrics()
{
	boost::json::object result;

	result["messages_consumed"] = consumerMetrics.messagesConsumed.load();
	result["bytes_consumed"] = consumerMetrics.bytesConsumed.load();
	result["errors_count"] = consumerMetrics.errorsCount.load();
	result["poll_timeouts"] = consumerMetrics.pollTimeouts.load();

	// Вычисляем время работы в секундах
	auto now = std::chrono::steady_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - consumerMetrics.startTime).count();
	result["uptime_seconds"] = duration;

	// Средняя скорость сообщений в секунду
	if (duration > 0)
	{
		result["messages_per_second"] = static_cast<double>(consumerMetrics.messagesConsumed.load()) / duration;
		result["bytes_per_second"] = static_cast<double>(consumerMetrics.bytesConsumed.load()) / duration;
	}
	else
	{
		result["messages_per_second"] = 0.0;
		result["bytes_per_second"] = 0.0;
	}

	return boost::json::serialize(result);
}

bool SimpleKafka1C::resetMetrics()
{
	producerMetrics.reset();
	consumerMetrics.reset();
	return true;
}

//================================== Schema Registry API ==========================================

// Callback для записи данных от CURL
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* userp)
{
	size_t totalSize = size * nmemb;
	userp->append(static_cast<char*>(contents), totalSize);
	return totalSize;
}

std::string SimpleKafka1C::httpRequest(const std::string& url, const std::string& method,
                                        const std::string& body, const std::string& contentType)
{
	std::string response;

	if (!curlHandle)
	{
		curlHandle = curl_easy_init();
	}

	if (!curlHandle)
	{
		msg_err = u8"Ошибка инициализации CURL";
		return "";
	}

	curl_easy_reset(curlHandle);

	curl_easy_setopt(curlHandle, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curlHandle, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curlHandle, CURLOPT_WRITEDATA, &response);
	curl_easy_setopt(curlHandle, CURLOPT_TIMEOUT, 30L);

	struct curl_slist* headers = nullptr;
	headers = curl_slist_append(headers, ("Content-Type: " + contentType).c_str());
	headers = curl_slist_append(headers, "Accept: application/vnd.schemaregistry.v1+json");
	curl_easy_setopt(curlHandle, CURLOPT_HTTPHEADER, headers);

	if (method == "POST")
	{
		curl_easy_setopt(curlHandle, CURLOPT_POST, 1L);
		curl_easy_setopt(curlHandle, CURLOPT_POSTFIELDS, body.c_str());
		curl_easy_setopt(curlHandle, CURLOPT_POSTFIELDSIZE, body.size());
	}
	else if (method == "DELETE")
	{
		curl_easy_setopt(curlHandle, CURLOPT_CUSTOMREQUEST, "DELETE");
	}
	else if (method == "PUT")
	{
		curl_easy_setopt(curlHandle, CURLOPT_CUSTOMREQUEST, "PUT");
		curl_easy_setopt(curlHandle, CURLOPT_POSTFIELDS, body.c_str());
		curl_easy_setopt(curlHandle, CURLOPT_POSTFIELDSIZE, body.size());
	}

	CURLcode res = curl_easy_perform(curlHandle);

	curl_slist_free_all(headers);

	if (res != CURLE_OK)
	{
		msg_err = std::string(u8"CURL ошибка: ") + curl_easy_strerror(res);
		return "";
	}

	long httpCode = 0;
	curl_easy_getinfo(curlHandle, CURLINFO_RESPONSE_CODE, &httpCode);

	if (httpCode >= 400)
	{
		msg_err = u8"HTTP ошибка " + std::to_string(httpCode) + ": " + response;
		return "";
	}

	return response;
}

std::string SimpleKafka1C::registerSchema(const variant_t& registryUrl, const variant_t& subject, const variant_t& schema)
{
	std::string url = std::get<std::string>(registryUrl);
	std::string subj = std::get<std::string>(subject);
	std::string schemaStr = std::get<std::string>(schema);

	// Убираем trailing slash
	if (!url.empty() && url.back() == '/')
		url.pop_back();

	std::string fullUrl = url + "/subjects/" + subj + "/versions";

	// Формируем JSON тело запроса
	boost::json::object requestBody;
	requestBody["schema"] = schemaStr;

	std::string body = boost::json::serialize(requestBody);

	std::string response = httpRequest(fullUrl, "POST", body, "application/vnd.schemaregistry.v1+json");

	return response;
}

std::string SimpleKafka1C::getSchemaById(const variant_t& registryUrl, const variant_t& schemaId)
{
	std::string url = std::get<std::string>(registryUrl);
	int32_t id = std::get<int32_t>(schemaId);

	// Убираем trailing slash
	if (!url.empty() && url.back() == '/')
		url.pop_back();

	std::string fullUrl = url + "/schemas/ids/" + std::to_string(id);

	return httpRequest(fullUrl, "GET");
}

std::string SimpleKafka1C::getLatestSchema(const variant_t& registryUrl, const variant_t& subject)
{
	std::string url = std::get<std::string>(registryUrl);
	std::string subj = std::get<std::string>(subject);

	// Убираем trailing slash
	if (!url.empty() && url.back() == '/')
		url.pop_back();

	std::string fullUrl = url + "/subjects/" + subj + "/versions/latest";

	return httpRequest(fullUrl, "GET");
}

std::string SimpleKafka1C::getSchemaVersions(const variant_t& registryUrl, const variant_t& subject)
{
	std::string url = std::get<std::string>(registryUrl);
	std::string subj = std::get<std::string>(subject);

	// Убираем trailing slash
	if (!url.empty() && url.back() == '/')
		url.pop_back();

	std::string fullUrl = url + "/subjects/" + subj + "/versions";

	return httpRequest(fullUrl, "GET");
}

bool SimpleKafka1C::deleteSchema(const variant_t& registryUrl, const variant_t& subject, const variant_t& version)
{
	std::string url = std::get<std::string>(registryUrl);
	std::string subj = std::get<std::string>(subject);
	std::string ver = std::get<std::string>(version);

	// Убираем trailing slash
	if (!url.empty() && url.back() == '/')
		url.pop_back();

	std::string fullUrl = url + "/subjects/" + subj + "/versions/" + ver;

	std::string response = httpRequest(fullUrl, "DELETE");

	return !response.empty();
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

// Вспомогательная функция для рекурсивного заполнения GenericDatum из JSON
static bool fillAvroFromJson(avro::GenericDatum& datum, const boost::json::value& jsonValue, std::string& errMsg, const std::string& path = "")
{
	// Обработка union типов
	if (datum.isUnion())
	{
		if (jsonValue.is_null())
		{
			// null - первая ветка union (обычно ["null", "type"])
			datum.selectBranch(0);
			return true;
		}
		// Не null - выбираем вторую ветку
		datum.selectBranch(1);
	}

	switch (datum.type())
	{
	case avro::AVRO_STRING:
		if (!jsonValue.is_string())
		{
			errMsg = "Expected string at " + path;
			return false;
		}
		datum.value<std::string>() = std::string(jsonValue.as_string());
		return true;

	case avro::AVRO_LONG:
		if (!jsonValue.is_int64())
		{
			errMsg = "Expected long at " + path;
			return false;
		}
		datum.value<int64_t>() = jsonValue.as_int64();
		return true;

	case avro::AVRO_INT:
		if (!jsonValue.is_int64())
		{
			errMsg = "Expected int at " + path;
			return false;
		}
		datum.value<int32_t>() = static_cast<int32_t>(jsonValue.as_int64());
		return true;

	case avro::AVRO_FLOAT:
		if (jsonValue.is_double())
			datum.value<float>() = static_cast<float>(jsonValue.as_double());
		else if (jsonValue.is_int64())
			datum.value<float>() = static_cast<float>(jsonValue.as_int64());
		else
		{
			errMsg = "Expected float at " + path;
			return false;
		}
		return true;

	case avro::AVRO_DOUBLE:
		if (jsonValue.is_double())
			datum.value<double>() = jsonValue.as_double();
		else if (jsonValue.is_int64())
			datum.value<double>() = static_cast<double>(jsonValue.as_int64());
		else
		{
			errMsg = "Expected double at " + path;
			return false;
		}
		return true;

	case avro::AVRO_BOOL:
		if (!jsonValue.is_bool())
		{
			errMsg = "Expected bool at " + path;
			return false;
		}
		datum.value<bool>() = jsonValue.as_bool();
		return true;

	case avro::AVRO_NULL:
		datum.value<avro::null>() = avro::null();
		return true;

	case avro::AVRO_BYTES:
	{
		if (!jsonValue.is_string())
		{
			errMsg = "Expected string (bytes) at " + path;
			return false;
		}
		std::string strVal = std::string(jsonValue.as_string());
		std::vector<uint8_t> bytes(strVal.begin(), strVal.end());
		datum.value<std::vector<uint8_t>>() = bytes;
		return true;
	}

	case avro::AVRO_FIXED:
	{
		if (!jsonValue.is_string())
		{
			errMsg = "Expected string (fixed) at " + path;
			return false;
		}
		avro::GenericFixed& fixed = datum.value<avro::GenericFixed>();
		std::string strVal = std::string(jsonValue.as_string());
		// Проверка на UUID формат
		if (strVal.length() == 36 && strVal[8] == '-' && strVal[13] == '-')
		{
			std::string hex;
			for (char c : strVal)
			{
				if (c != '-') hex += c;
			}
			std::vector<uint8_t>& bytes = fixed.value();
			bytes.resize(16);
			for (size_t j = 0; j < 16; j++)
			{
				std::string byteStr = hex.substr(j * 2, 2);
				bytes[j] = static_cast<uint8_t>(std::stoul(byteStr, nullptr, 16));
			}
		}
		else
		{
			std::vector<uint8_t>& bytes = fixed.value();
			bytes.assign(strVal.begin(), strVal.end());
		}
		return true;
	}

	case avro::AVRO_RECORD:
	{
		if (!jsonValue.is_object())
		{
			errMsg = "Expected object at " + path;
			return false;
		}
		const boost::json::object& obj = jsonValue.as_object();
		avro::GenericRecord& record = datum.value<avro::GenericRecord>();
		size_t fieldCount = record.fieldCount();
		const avro::NodePtr& schema = record.schema();

		for (size_t i = 0; i < fieldCount; ++i)
		{
			std::string fieldName = schema->nameAt(i);
			auto it = obj.find(fieldName);
			if (it == obj.end())
			{
				// Поле отсутствует - проверяем, есть ли default или это union с null
				avro::GenericDatum& fieldDatum = record.fieldAt(i);
				if (fieldDatum.isUnion())
				{
					// Union с null - устанавливаем null
					fieldDatum.selectBranch(0);
				}
				// Иначе оставляем значение по умолчанию
				continue;
			}
			avro::GenericDatum& fieldDatum = record.fieldAt(i);
			std::string fieldPath = path.empty() ? fieldName : path + "." + fieldName;
			if (!fillAvroFromJson(fieldDatum, it->value(), errMsg, fieldPath))
			{
				return false;
			}
		}
		return true;
	}

	case avro::AVRO_ENUM:
	{
		if (!jsonValue.is_string())
		{
			errMsg = "Expected string (enum symbol) at " + path;
			return false;
		}
		avro::GenericEnum& enumVal = datum.value<avro::GenericEnum>();
		std::string symbol = std::string(jsonValue.as_string());
		enumVal.set(symbol);
		return true;
	}

	case avro::AVRO_ARRAY:
	{
		if (!jsonValue.is_array())
		{
			errMsg = "Expected array at " + path;
			return false;
		}
		const boost::json::array& arr = jsonValue.as_array();
		avro::GenericArray& avroArray = datum.value<avro::GenericArray>();
		const avro::NodePtr& schema = avroArray.schema();
		avroArray.value().clear();

		for (size_t i = 0; i < arr.size(); ++i)
		{
			avro::GenericDatum elemDatum(schema->leafAt(0));
			std::string elemPath = path + "[" + std::to_string(i) + "]";
			if (!fillAvroFromJson(elemDatum, arr[i], errMsg, elemPath))
			{
				return false;
			}
			avroArray.value().push_back(elemDatum);
		}
		return true;
	}

	case avro::AVRO_MAP:
	{
		if (!jsonValue.is_object())
		{
			errMsg = "Expected object (map) at " + path;
			return false;
		}
		const boost::json::object& obj = jsonValue.as_object();
		avro::GenericMap& avroMap = datum.value<avro::GenericMap>();
		const avro::NodePtr& schema = avroMap.schema();
		avroMap.value().clear();

		for (const auto& kv : obj)
		{
			avro::GenericDatum valueDatum(schema->leafAt(1));
			std::string elemPath = path + "[\"" + std::string(kv.key()) + "\"]";
			if (!fillAvroFromJson(valueDatum, kv.value(), errMsg, elemPath))
			{
				return false;
			}
			avroMap.value().push_back(std::make_pair(std::string(kv.key()), valueDatum));
		}
		return true;
	}

	default:
		errMsg = "Unsupported Avro type at " + path;
		return false;
	}
}

bool SimpleKafka1C::convertToAvroFormat(const variant_t& msgJson, const variant_t& schemaJsonName)
{
	avroFile.clear();
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
		msg_err = u8"Некорректная схема - корневой тип должен быть record";
		return false;
	}

	// Разбираем исходный JSON
	boost::json::monotonic_resource mr;
	boost::json::value jsonInput;
	try
	{
		jsonInput = boost::json::parse(std::get<std::string>(msgJson), &mr);
	}
	catch (std::exception const& ex)
	{
		msg_err = "Error parsing JSON - ";
		msg_err += ex.what();
		return false;
	}

	MemoryOutputStream* memOutStr = new MemoryOutputStream(100000);
	std::unique_ptr<avro::OutputStream> os(memOutStr);
	avro::DataFileWriter<avro::GenericDatum> writer(std::move(os), schema);

	try
	{
		// Определяем формат входных данных:
		// 1. Объект {...} - одна запись (новый стандартный формат)
		// 2. Массив [{...}, {...}] - несколько записей (новый стандартный формат)
		// 3. "Столбцовый" формат {"field1": [v1, v2], "field2": [v1, v2]} - для совместимости

		if (jsonInput.is_object())
		{
			const boost::json::object& obj = jsonInput.as_object();

			// Проверяем формат: если первое значение - массив, это "столбцовый" формат
			bool isColumnarFormat = false;
			if (!obj.empty())
			{
				const auto& firstValue = obj.cbegin()->value();
				if (firstValue.is_array())
				{
					// Дополнительная проверка: в стандартном формате array тоже может быть значением поля
					// Проверяем, совпадают ли размеры всех массивов (признак столбцового формата)
					size_t firstSize = firstValue.as_array().size();
					bool allSameSize = true;
					for (const auto& kv : obj)
					{
						if (!kv.value().is_array() || kv.value().as_array().size() != firstSize)
						{
							allSameSize = false;
							break;
						}
					}
					// Если все поля - массивы одинаковой длины, считаем это столбцовым форматом
					// (кроме случая когда длина = 1, тогда это может быть и стандартный формат)
					isColumnarFormat = allSameSize && firstSize > 1;
				}
			}

			if (isColumnarFormat)
			{
				// Старый "столбцовый" формат для совместимости
				const auto first_array = obj.cbegin();
				const size_t numElements = first_array->value().as_array().size();

				for (size_t i = 0; i < numElements; i++)
				{
					boost::json::object jsonRecord;
					for (const auto& kv : obj)
					{
						jsonRecord[kv.key()] = kv.value().as_array().at(i);
					}

					avro::GenericDatum recordDatum(schema);
					if (!fillAvroFromJson(recordDatum, boost::json::value(jsonRecord), msg_err))
					{
						return false;
					}
					writer.write(recordDatum);
				}
			}
			else
			{
				// Стандартный формат: одна запись как объект
				if (!fillAvroFromJson(datum, jsonInput, msg_err))
				{
					return false;
				}
				writer.write(datum);
			}
		}
		else if (jsonInput.is_array())
		{
			// Стандартный формат: массив записей
			const boost::json::array& arr = jsonInput.as_array();
			for (size_t i = 0; i < arr.size(); ++i)
			{
				avro::GenericDatum recordDatum(schema);
				std::string recordPath = "[" + std::to_string(i) + "]";
				if (!fillAvroFromJson(recordDatum, arr[i], msg_err, recordPath))
				{
					return false;
				}
				writer.write(recordDatum);
			}
		}
		else
		{
			msg_err = u8"JSON должен быть объектом или массивом объектов";
			return false;
		}

		writer.flush();
		memOutStr->snapshot(avroFile);
	}
	catch (std::exception const& ex)
	{
		msg_err += "Error converting to Avro: ";
		msg_err += ex.what();
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
	case avro::AVRO_RECORD:
	{
		// Рекурсивная обработка вложенных record
		boost::json::object obj;
		const avro::GenericRecord& record = datum.value<avro::GenericRecord>();
		const avro::NodePtr& schema = record.schema();
		size_t fieldCount = record.fieldCount();
		for (size_t i = 0; i < fieldCount; ++i)
		{
			std::string fieldName = schema->nameAt(i);
			const avro::GenericDatum& fieldDatum = record.fieldAt(i);
			obj[fieldName] = convertAvroDatumToJson(fieldDatum);
		}
		return obj;
	}
	case avro::AVRO_ENUM:
	{
		// Enum возвращается как строка-символ
		const avro::GenericEnum& enumVal = datum.value<avro::GenericEnum>();
		return boost::json::string(enumVal.symbol());
	}
	case avro::AVRO_ARRAY:
	{
		// Массив элементов
		boost::json::array arr;
		const avro::GenericArray& avroArray = datum.value<avro::GenericArray>();
		for (const auto& elem : avroArray.value())
		{
			arr.push_back(convertAvroDatumToJson(elem));
		}
		return arr;
	}
	case avro::AVRO_MAP:
	{
		// Map как JSON объект
		boost::json::object obj;
		const avro::GenericMap& avroMap = datum.value<avro::GenericMap>();
		for (const auto& pair : avroMap.value())
		{
			obj[pair.first] = convertAvroDatumToJson(pair.second);
		}
		return obj;
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
			// Конвертируем в JSON (стандартный формат - массив объектов или один объект)
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

			// Если одна запись - возвращаем объект, если несколько - массив
			if (records.size() == 1)
			{
				// Одна запись - возвращаем как объект
				boost::json::value result = convertAvroDatumToJson(records[0]);
				return boost::json::serialize(result);
			}
			else
			{
				// Несколько записей - возвращаем как массив
				boost::json::array resultArray;
				for (const auto& record : records)
				{
					resultArray.push_back(convertAvroDatumToJson(record));
				}
				return boost::json::serialize(resultArray);
			}
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
