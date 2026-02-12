#include <boost/algorithm/string/split.hpp>          // boost::algorithm::split
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/json.hpp>
#ifdef _WINDOWS
#include <process.h>
#endif

#include <thread>
#include <chrono>
#include <set>
#include <atomic>
#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>
#include <cerrno>
#include <cstring>

#include "md5.h"
#include "SimpleKafka1C.h"
#include "utils.h"

#ifdef SIMPLEKAFKA_HAS_OPENSSL
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/provider.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#endif

// Protobuf helpers moved to protobuf_methods.cpp

namespace {
std::string trimCopy(const std::string& value)
{
	size_t begin = 0;
	size_t end = value.size();
	while (begin < end && std::isspace(static_cast<unsigned char>(value[begin])) != 0)
	{
		++begin;
	}
	while (end > begin && std::isspace(static_cast<unsigned char>(value[end - 1])) != 0)
	{
		--end;
	}
	return value.substr(begin, end - begin);
}

std::string toLowerCopy(std::string value)
{
	std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
		return static_cast<char>(std::tolower(c));
	});
	return value;
}

bool readTextFile(const std::string& path, std::string& content, std::string& error)
{
	std::ifstream input(path, std::ios::in | std::ios::binary);
	if (!input.is_open())
	{
		error = "Cannot open file: " + path + " (" + std::strerror(errno) + ")";
		return false;
	}

	std::ostringstream buffer;
	buffer << input.rdbuf();
	if (input.bad())
	{
		error = "Cannot read file: " + path;
		return false;
	}

	content = buffer.str();
	if (content.empty())
	{
		error = "File is empty: " + path;
		return false;
	}

	return true;
}

#ifdef SIMPLEKAFKA_HAS_OPENSSL
std::string collectOpenSslErrors()
{
	std::string details;
	for (unsigned long err = ERR_get_error(); err != 0; err = ERR_get_error())
	{
		char text[256] = {0};
		ERR_error_string_n(err, text, sizeof(text));
		if (!details.empty())
		{
			details += "; ";
		}
		details += text;
	}
	return details;
}

bool containsWeakSha1Cert(const std::string& pemPath, std::string& details)
{
	if (pemPath.empty())
	{
		return false;
	}

	BIO* bio = BIO_new_file(pemPath.c_str(), "r");
	if (bio == nullptr)
	{
		return false;
	}

	bool found = false;
	while (true)
	{
		X509* cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
		if (cert == nullptr)
		{
			break;
		}

		const int sigNid = X509_get_signature_nid(cert);
		const char* sigName = OBJ_nid2sn(sigNid);
		if (sigName != nullptr)
		{
			std::string sign = toLowerCopy(sigName);
			if (sign.find("sha1") != std::string::npos)
			{
				found = true;
				if (details.empty())
				{
					details = pemPath + " uses weak signature algorithm: " + sigName;
				}
			}
		}
		X509_free(cert);
	}

	BIO_free(bio);
	ERR_clear_error();
	return found;
}
#endif
} // namespace

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

			eventFile.open(logDir + bufnameCS + std::to_string(pid) + "_" + currentDateTime(formatLogFiles.c_str()) + ".log", std::ios_base::app);
		
		if (statisticsOn) {
				statFile.open(logDir + bufnameST + std::to_string(pid) + "_" + currentDateTime(formatLogFiles.c_str()) + ".log", std::ios_base::app);
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
		eventFile << currentDateTime() << " LOG-" << event.severity() << "-"
			<< event.fac() << ": " << event.str() << std::endl;
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
			eventFile.open(logDir + bufname + std::to_string(pid) + "_" + currentDateTime(formatLogFiles.c_str()) + ".log", std::ios_base::app);
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

		if (message.key() != nullptr && message.key()->length())
		{
			eventFile << " Key:" << *(message.key()) << ", ";
		}
		else if (message.payload() != nullptr && message.len() > 0)
		{
			MD5 payloadHash;
			payloadHash.update(static_cast<const char*>(message.payload()),
			                   static_cast<MD5::size_type>(message.len()));
			payloadHash.finalize();
			eventFile << " PayloadMD5:" << payloadHash.hexdigest() << ", ";
		}
		else
		{
			eventFile << " PayloadMD5:<empty>, ";
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
	AddMethod(L"ConvertToAvroFormat", L"ПреобразоватьВФорматAVRO", this, &SimpleKafka1C::convertToAvroFormat,
		{ {2, std::string("")}, {3, int32_t(0)} });
	AddMethod(L"ProduceAvro", L"ОтправитьСообщениеAVRO", this, &SimpleKafka1C::produceAvro,
		{ {1, -1}, {3, std::string("")}, {4, std::string("")} });
	AddMethod(L"ProduceAvroWithWaitResult", L"ОтправитьСообщениеAVROСОжиданиемРезультата", this, &SimpleKafka1C::produceAvroWithWaitResult,
		{ {1, -1}, {3, std::string("")}, {4, std::string("")} });
	AddMethod(L"SaveAvroFile", L"СохранитьФайлAVRO", this, &SimpleKafka1C::saveAvroFile);
	AddMethod(L"DecodeAvroMessage", L"ДекодироватьСообщениеAVRO", this, &SimpleKafka1C::decodeAvroMessage,
		{ {1, std::string("")}, {2, true} });
	AddMethod(L"GetAvroSchema", L"ПолучитьСхемуAVRO", this, &SimpleKafka1C::getAvroSchema);

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

	// Очистка rebalance callback offsets для предотвращения утечки памяти
	for (auto* offset : cl_rebalance_cb.offsets)
	{
		delete offset;
	}
	cl_rebalance_cb.offsets.clear();

#ifdef SIMPLEKAFKA_HAS_OPENSSL
	for (OSSL_PROVIDER* provider : loadedSslProviderHandles)
	{
		(void)provider;
	}
	loadedSslProviderHandles.clear();
#endif

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

std::string SimpleKafka1C::getSettingValue(std::string_view key) const
{
	for (auto it = settings.rbegin(); it != settings.rend(); ++it)
	{
		if (it->Key == key)
		{
			return it->Value;
		}
	}
	return "";
}

bool SimpleKafka1C::hasSettingKey(std::string_view key) const
{
	for (auto it = settings.rbegin(); it != settings.rend(); ++it)
	{
		if (it->Key == key)
		{
			return true;
		}
	}
	return false;
}

bool SimpleKafka1C::isSslProtocolConfigured() const
{
	const std::string securityProtocol = toLowerCopy(trimCopy(getSettingValue("security.protocol")));
	return securityProtocol == "ssl" || securityProtocol == "sasl_ssl";
}

std::string SimpleKafka1C::enrichSslError(std::string_view baseError) const
{
	std::string error(baseError);
#ifdef SIMPLEKAFKA_HAS_OPENSSL
	std::string sslDetails = collectOpenSslErrors();
	if (!sslDetails.empty())
	{
		if (!error.empty())
		{
			error += " | ";
		}
		error += "OpenSSL: ";
		error += sslDetails;
	}
#endif
	return error;
}

bool SimpleKafka1C::prepareSslRuntime(std::string& error)
{
#if defined(_WIN32)
	(void)error;
	return true;
#endif

	if (!isSslProtocolConfigured())
	{
		return true;
	}

#ifndef SIMPLEKAFKA_HAS_OPENSSL
	error = "SSL requested, but component was built without OpenSSL runtime integration";
	return false;
#else
	std::lock_guard<std::mutex> lock(sslRuntimeMutex);
	ERR_clear_error();

	const std::string verification = toLowerCopy(trimCopy(getSettingValue("enable.ssl.certificate.verification")));
	const bool verificationDisabled = (verification == "false" || verification == "0");
	const std::string ciphers = toLowerCopy(getSettingValue("ssl.cipher.suites"));
	const bool explicitSeclevelOverride =
	    (ciphers.find("@seclevel=0") != std::string::npos || ciphers.find("@seclevel=1") != std::string::npos);

	// Guard against known OpenSSL3/SECLEVEL crashes with legacy SHA1 chains.
	if (!verificationDisabled && !explicitSeclevelOverride)
	{
		std::string weakDetails;
		const std::string caLocation = trimCopy(getSettingValue("ssl.ca.location"));
		const std::string certLocation = trimCopy(getSettingValue("ssl.certificate.location"));
		bool hasWeakSha1 = false;
		hasWeakSha1 = containsWeakSha1Cert(caLocation, weakDetails) || hasWeakSha1;
		hasWeakSha1 = containsWeakSha1Cert(certLocation, weakDetails) || hasWeakSha1;
		if (hasWeakSha1)
		{
			error =
			    "TLS init blocked: weak SHA1 certificate detected (" + weakDetails +
			    "). On OpenSSL 3 with SECLEVEL=2 this may fail or crash. "
			    "Use stronger certificates, or explicitly lower SECLEVEL in ssl.cipher.suites, "
			    "or disable certificate verification.";
			return false;
		}
	}

	const std::string providers = trimCopy(getSettingValue("ssl.providers"));
	if (!providers.empty())
	{
		std::vector<std::string> parsedProviders;
		boost::algorithm::split(parsedProviders, providers, boost::is_any_of(","));
		for (std::string providerName : parsedProviders)
		{
			providerName = trimCopy(providerName);
			if (providerName.empty())
			{
				continue;
			}
			if (std::find(loadedSslProviders.begin(), loadedSslProviders.end(), providerName) != loadedSslProviders.end())
			{
				continue;
			}
			OSSL_PROVIDER* provider = OSSL_PROVIDER_load(nullptr, providerName.c_str());
			if (provider == nullptr)
			{
				error = "OpenSSL provider load failed: " + providerName;
				error = enrichSslError(error);
				return false;
			}
			loadedSslProviders.push_back(providerName);
			loadedSslProviderHandles.push_back(provider);
		}
	}

	std::string sigalgs = trimCopy(getSettingValue("ssl.sigalgs.list"));
	if (!sigalgs.empty())
	{
		SSL_CTX* sslCtx = SSL_CTX_new(TLS_client_method());
		if (sslCtx == nullptr)
		{
			error = enrichSslError("SSL_CTX_new() failed during ssl.sigalgs.list validation");
			return false;
		}

		const int setRes = SSL_CTX_set1_sigalgs_list(sslCtx, sigalgs.c_str());
		SSL_CTX_free(sslCtx);
		if (setRes != 1)
		{
			error = "Invalid ssl.sigalgs.list value: " + sigalgs;
			error = enrichSslError(error);
			return false;
		}
	}

	return true;
#endif
}

bool SimpleKafka1C::applyKafkaSettings(RdKafka::Conf* conf, std::string& error, bool* statisticsOn)
{
	if (!prepareSslRuntime(error))
	{
		return false;
	}

#if defined(__linux__) && !defined(_WIN32)
	const bool linuxSsl = isSslProtocolConfigured();
#endif

	for (const auto& setting : settings)
	{
#if defined(_WIN32)
		// Linux-only tuning knobs: do not apply on Windows.
		if (setting.Key == "ssl.providers" || setting.Key == "ssl.sigalgs" || setting.Key == "ssl.sigalgs.list")
		{
			continue;
		}
#endif
#if defined(__linux__) && !defined(_WIN32)
		// On Linux feed PEM contents via ssl.*.pem to avoid file-based SSL_CTX_use_certificate_chain_file path.
		if (linuxSsl && (setting.Key == "ssl.ca.location" || setting.Key == "ssl.certificate.location" || setting.Key == "ssl.key.location"))
		{
			continue;
		}
#endif

		if (conf->set(setting.Key, setting.Value, error) != RdKafka::Conf::CONF_OK)
		{
			error = enrichSslError(error);
			return false;
		}
		if (statisticsOn != nullptr && setting.Key == "statistics.interval.ms")
		{
			*statisticsOn = true;
		}
	}

#if defined(__linux__) && !defined(_WIN32)
	if (isSslProtocolConfigured() && getSettingValue("ssl.providers").empty())
	{
		if (conf->set("ssl.providers", "default,legacy", error) != RdKafka::Conf::CONF_OK)
		{
			error = enrichSslError(error);
			return false;
		}
	}
#endif

#if defined(__linux__) && !defined(_WIN32)
	if (linuxSsl)
	{
		auto setPemFromLocation = [&](const std::string& locationKey, const std::string& pemKey) -> bool {
			if (hasSettingKey(pemKey))
			{
				return true;
			}

			const std::string location = trimCopy(getSettingValue(locationKey));
			if (location.empty())
			{
				return true;
			}

			std::string pem;
			if (!readTextFile(location, pem, error))
			{
				error = enrichSslError(error);
				return false;
			}

			if (conf->set(pemKey, pem, error) != RdKafka::Conf::CONF_OK)
			{
				error = enrichSslError(error);
				return false;
			}
			return true;
		};

		if (!setPemFromLocation("ssl.ca.location", "ssl.ca.pem") ||
		    !setPemFromLocation("ssl.certificate.location", "ssl.certificate.pem") ||
		    !setPemFromLocation("ssl.key.location", "ssl.key.pem"))
		{
			return false;
		}
	}
#endif

	return true;
}

bool SimpleKafka1C::applyKafkaSettings(rd_kafka_conf_t* conf, std::string& error)
{
	if (!prepareSslRuntime(error))
	{
		return false;
	}

	char errstr[512] = {0};
#if defined(__linux__) && !defined(_WIN32)
	const bool linuxSsl = isSslProtocolConfigured();
#endif
	for (const auto& setting : settings)
	{
#if defined(_WIN32)
		// Linux-only tuning knobs: do not apply on Windows.
		if (setting.Key == "ssl.providers" || setting.Key == "ssl.sigalgs" || setting.Key == "ssl.sigalgs.list")
		{
			continue;
		}
#endif
#if defined(__linux__) && !defined(_WIN32)
		// On Linux feed PEM contents via ssl.*.pem to avoid file-based SSL_CTX_use_certificate_chain_file path.
		if (linuxSsl && (setting.Key == "ssl.ca.location" || setting.Key == "ssl.certificate.location" || setting.Key == "ssl.key.location"))
		{
			continue;
		}
#endif

		if (rd_kafka_conf_set(conf, setting.Key.c_str(), setting.Value.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			error = enrichSslError(errstr);
			return false;
		}
	}

#if defined(__linux__) && !defined(_WIN32)
	if (isSslProtocolConfigured() && getSettingValue("ssl.providers").empty())
	{
		if (rd_kafka_conf_set(conf, "ssl.providers", "default,legacy", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			error = enrichSslError(errstr);
			return false;
		}
	}
#endif

#if defined(__linux__) && !defined(_WIN32)
	if (linuxSsl)
	{
		auto setPemFromLocation = [&](const std::string& locationKey, const std::string& pemKey) -> bool {
			if (hasSettingKey(pemKey))
			{
				return true;
			}

			const std::string location = trimCopy(getSettingValue(locationKey));
			if (location.empty())
			{
				return true;
			}

			std::string pem;
			if (!readTextFile(location, pem, error))
			{
				error = enrichSslError(error);
				return false;
			}

			if (rd_kafka_conf_set(conf, pemKey.c_str(), pem.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
			{
				error = enrichSslError(errstr);
				return false;
			}
			return true;
		};

		if (!setPemFromLocation("ssl.ca.location", "ssl.ca.pem") ||
		    !setPemFromLocation("ssl.certificate.location", "ssl.certificate.pem") ||
		    !setPemFromLocation("ssl.key.location", "ssl.key.pem"))
		{
			return false;
		}
	}
#endif

	return true;
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
			eventFile.open(cl_event_cb.logDir + bufname + std::to_string(pid) + "_" + currentDateTime(cl_event_cb.formatLogFiles.c_str()) + ".log", std::ios_base::app);
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

// Schema Registry API methods moved to schema_registry_methods.cpp
// Avro methods moved to avro_methods.cpp
// Protobuf methods moved to protobuf_methods.cpp
