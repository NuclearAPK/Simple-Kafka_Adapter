#ifndef SIMPLEKAFKA1C_H
#define SIMPLEKAFKA1C_H

#include <librdkafka/rdkafkacpp.h>
#include <librdkafka/rdkafka.h>
#include <avro/ValidSchema.hh>
#include <avro/Stream.hh>
#include <avro/LogicalType.hh>
#include <curl/curl.h>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string_view>
#include "Component.h"

#ifdef SIMPLEKAFKA_HAS_OPENSSL
#include <openssl/provider.h>
#endif

// RAII wrapper for RdKafka::Conf to prevent memory leaks
struct RdKafkaConfDeleter {
	void operator()(RdKafka::Conf* conf) const { delete conf; }
};
using RdKafkaConfPtr = std::unique_ptr<RdKafka::Conf, RdKafkaConfDeleter>;

class SimpleKafka1C final : public Component
{
public:
	static constexpr char Version[] = u8"1.8.5";

	SimpleKafka1C();
	~SimpleKafka1C();

private:
	static constexpr char EMPTYSTR[] = u8"";

	// property
	std::shared_ptr<variant_t> logDirectory;
	std::shared_ptr<variant_t> formatLogFiles;

	RdKafka::Producer *hProducer;
	RdKafka::KafkaConsumer *hConsumer;

	int32_t waitMessageTimeout;
	int32_t producerFlushTimeout = 20000;
	int32_t consumerCloseTimeout = 10000;
	int32_t adminOperationTimeout = 10000;
	std::string partitionerStrategy = "consistent_random";
	CURL* curlHandle = nullptr;
	unsigned pid;

	static constexpr char consumerLogName[] = "consumer_";
	static constexpr char producerLogName[] = "producer_";
	static constexpr char statLogName[] = "statistics_";

	// message
	std::string key;
	std::string topic;
	int32_t broker_id;
	int64_t timestamp;
	int32_t partition;
	int64_t offset;
	std::vector<char> messageData;
	size_t messageLen;

	struct HeadersMessage
	{
		std::string Key;
		std::string Value;
	};

	// Metrics structures
	struct ProducerMetrics {
		std::atomic<uint64_t> messagesProduced{0};
		std::atomic<uint64_t> bytesProduced{0};
		std::atomic<uint64_t> errorsCount{0};
		std::atomic<uint64_t> retriesCount{0};
		std::chrono::steady_clock::time_point startTime;

		ProducerMetrics() : startTime(std::chrono::steady_clock::now()) {}

		void reset() {
			messagesProduced = 0;
			bytesProduced = 0;
			errorsCount = 0;
			retriesCount = 0;
			startTime = std::chrono::steady_clock::now();
		}
	};

	struct ConsumerMetrics {
		std::atomic<uint64_t> messagesConsumed{0};
		std::atomic<uint64_t> bytesConsumed{0};
		std::atomic<uint64_t> errorsCount{0};
		std::atomic<uint64_t> pollTimeouts{0};
		std::chrono::steady_clock::time_point startTime;

		ConsumerMetrics() : startTime(std::chrono::steady_clock::now()) {}

		void reset() {
			messagesConsumed = 0;
			bytesConsumed = 0;
			errorsCount = 0;
			pollTimeouts = 0;
			startTime = std::chrono::steady_clock::now();
		}
	};

	ProducerMetrics producerMetrics;
	ConsumerMetrics consumerMetrics;

	std::vector<HeadersMessage> messageHeaders;

	// avro
	std::map<std::string, avro::ValidSchema> schemesMap;	// кеш для хранение компилированных схем Avro
	std::vector<uint8_t> avroFile;		// формируемый avro

	// protobuf - using forward declarations to avoid including protobuf headers in .h
	class ProtobufContext;	// forward declaration
	std::shared_ptr<ProtobufContext> protoContext;	// контекст для работы с protobuf
	std::string protobufData;		// формируемый protobuf

	// parameters set
	// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	void setParameter(const variant_t &key, const variant_t &value);
	std::string getParameters();
	bool setPartitioner(const variant_t &partitionerType);

		std::string clientID();
		std::string extensionName() override;
		bool isSslProtocolConfigured() const;
		std::string getSettingValue(std::string_view key) const;
		bool hasSettingKey(std::string_view key) const;
		bool prepareSslRuntime(std::string& error);
		bool applyKafkaSettings(RdKafka::Conf* conf, std::string& error, bool* statisticsOn = nullptr);
		bool applyKafkaSettings(rd_kafka_conf_t* conf, std::string& error);
		std::string enrichSslError(std::string_view baseError) const;

		// Helper: parse "key1,val1;key2,val2" header string into RdKafka::Headers
		RdKafka::Headers* parseKafkaHeaders(const std::string& headerString);

		// Helper: retry produce on ERR__QUEUE_FULL with max retries
		static constexpr int MAX_QUEUE_FULL_RETRIES = 60;
		RdKafka::ErrorCode produceWithRetry(std::function<RdKafka::ErrorCode()> produceFn,
		                                     std::ofstream& eventFile);

		// Helper: create a temporary Producer for metadata/admin queries
		std::unique_ptr<RdKafka::Producer> createMetadataClient(const std::string& brokers);

		// Helper: common logic for produceWithWaitResult / produceAvroWithWaitResult / produceProtobufWithWaitResult
		int32_t produceAndWaitResult(std::function<int32_t()> produceFn, const std::string& methodLogName);

		// Helper: prepare filtered settings for applyKafkaSettings overloads
		struct FilteredSettings {
			struct Param { std::string key; std::string value; };
			std::vector<Param> params;
			std::vector<Param> pemOverrides;  // ssl.*.pem read from files
			bool hasStatisticsInterval = false;
			bool needDefaultSslProviders = false;
		};
		bool prepareFilteredSettings(FilteredSettings& out, std::string& error);

	// producer
	bool initProducer(const variant_t &brokers);
	int32_t produce(const variant_t &msg, const variant_t &topicName, const variant_t &partition, const variant_t &key, const variant_t &heads);
	int32_t produceWithWaitResult(const variant_t &msg, const variant_t &topicName, const variant_t &partition, const variant_t &key, const variant_t &heads);
	int32_t produceAvro(const variant_t &topicName, const variant_t &partition, const variant_t &key, const variant_t &heads);
	int32_t produceAvroWithWaitResult(const variant_t &topicName, const variant_t &partition, const variant_t &key, const variant_t &heads);
	int32_t produceBatch(const variant_t &messagesJson, const variant_t &topicName);
	bool stopProducer();

	// transactional producer
	bool initTransactionalProducer(const variant_t &brokers, const variant_t &transactionalId);
	bool beginTransaction();
	bool commitTransaction();
	bool abortTransaction();
	bool sendOffsetsToTransaction(const variant_t &offsetsJson, const variant_t &consumerGroupId);

	// consumer
	void clearMessageMetadata(); 
	bool initConsumer(const variant_t& brokers);
	bool subscribe(const variant_t& topic);
	std::string consume();	// устарела. рекомендуется использовать getMessage + getMessageMetadata + getMessageData
	bool getMessage();	// чтение с подтверждением
	std::string consumeBatch(const variant_t &maxMessages, const variant_t &maxWaitMs, const variant_t &asBase64);	// пакетное чтение
	variant_t getMessageData(const variant_t &binaryResult);	// данные, как они есть в kafka
	std::string getMessageKey();
	std::string getMessageHeaders();
	int32_t getMessageOffset();
	std::string getMessageTopicName();
	int32_t getMessageBrokerID();
	double getMessageTimestamp();
	std::string getMessageTimestampISO();
	int32_t getMessagePartition();

	bool commitOffset(const variant_t &topicName, const variant_t &offset, const variant_t &partition);
	bool setReadingPosition(const variant_t &topicName, const variant_t &offset, const variant_t &partition);
	bool setReadingPositions(const variant_t& jsonTopicPartitions);
	bool stopConsumer();
	bool setWaitingTimeout(const variant_t &timeout);
	bool setProducerFlushTimeout(const variant_t &timeout);
	bool setConsumerCloseTimeout(const variant_t &timeout);
	bool setAdminOperationTimeout(const variant_t &timeout);

	// consumer assignment (manual partition assignment)
	bool assign(const variant_t& jsonTopicPartitions);
	std::string getAssignment();
	bool unassign();

	// admin
	std::string getListOfTopics(const variant_t& brokers);
	std::string getTopicMetadata(const variant_t& brokers, const variant_t& topicName, const variant_t& timeout);
	std::string getConsumerCurrentGroupOffset(const variant_t& times, const variant_t& timeout);
	std::string getConsumerGroupOffsets(const variant_t& brokers, const variant_t& times, const variant_t& timeout);
	bool createTopic(const variant_t& brokers, const variant_t& topicName, const variant_t& partition, const variant_t& replication_factor);
	bool deleteTopic(const variant_t& brokers, const variant_t& topicName);
	bool deleteRecords(const variant_t& brokers, const variant_t& topicName, const variant_t& partitionsJson, const variant_t& timeout);
	std::string getTopicConfig(const variant_t& brokers, const variant_t& topicName, const variant_t& timeout);
	bool setTopicConfig(const variant_t& brokers, const variant_t& topicName, const variant_t& configJson, const variant_t& timeout);
	std::string getConsumerLag(const variant_t& brokers, const variant_t& topicName, const variant_t& consumerGroup, const variant_t& timeout);
	std::string getTopicConsumerGroups(const variant_t& brokers, const variant_t& topicName, const variant_t& timeout);

	// cluster and broker information
	std::string getClusterInfo(const variant_t& brokers);
	std::string getBrokerInfo(const variant_t& brokers, const variant_t& brokerId);
	std::string getPartitionWatermarks(const variant_t& brokers, const variant_t& topicName, const variant_t& partition);
	bool pingBroker(const variant_t& brokers, const variant_t& timeout);
	double getPartitionMessageCount(const variant_t& brokers, const variant_t& topicName, const variant_t& partition);
	std::string getBuiltinFeatures();
	std::string getTopicSize(const variant_t& brokers, const variant_t& topicName, const variant_t& timeout);

	// consumer group management
	bool deleteConsumerGroup(const variant_t& brokers, const variant_t& groupId);
	bool resetConsumerGroupOffsets(const variant_t& brokers, const variant_t& groupId, const variant_t& topicName, const variant_t& resetTo);

	// advanced consumer position management
	bool seekToBeginning(const variant_t& topicName, const variant_t& partition);
	bool seekToEnd(const variant_t& topicName, const variant_t& partition);
	bool seekToTimestamp(const variant_t& topicName, const variant_t& partition, const variant_t& timestamp);

	// Utilites
	bool sleep(const variant_t &delay);
	bool setLogDirectory(const variant_t& logDir);
	bool setFormatLogFiles(const variant_t& format);
	std::string getLastError() { return msg_err; }
	void openEventFile(const std::string& logName, std::ofstream& eventFile);

	// Metrics API
	std::string getProducerMetrics();
	std::string getConsumerMetrics();
	bool resetMetrics();

	// Schema Registry API
	std::string httpRequest(const std::string& url, const std::string& method,
	                        const std::string& body = "", const std::string& contentType = "application/json");
	std::string registerSchema(const variant_t& registryUrl, const variant_t& subject, const variant_t& schema);
	std::string getSchemaById(const variant_t& registryUrl, const variant_t& schemaId);
	std::string getLatestSchema(const variant_t& registryUrl, const variant_t& subject);
	std::string getSchemaVersions(const variant_t& registryUrl, const variant_t& subject);
	bool deleteSchema(const variant_t& registryUrl, const variant_t& subject, const variant_t& version);

	// converting a message to avro format
	bool putAvroSchema(const variant_t &schemaJsonName, const variant_t &schemaJson);
	bool convertToAvroFormat(const variant_t &msgJson, const variant_t &schemaJsonName, const variant_t &format, const variant_t &schemaId);
	bool saveAvroFile(const variant_t &fileName);
	variant_t decodeAvroMessage(const variant_t &avroData, const variant_t &schemaJsonName, const variant_t &asJson);
	variant_t getAvroSchema(const variant_t &avroData);

	// converting a message to protobuf format
	bool putProtoSchema(const variant_t &schemaName, const variant_t &protoSchema);
	bool convertToProtobufFormat(const variant_t &msgJson, const variant_t &schemaName);
	bool saveProtobufFile(const variant_t &fileName);
	variant_t decodeProtobufMessage(const variant_t &protobufData, const variant_t &schemaName, const variant_t &asJson);
	int32_t produceProtobuf(const variant_t &topicName, const variant_t &partition, const variant_t &key, const variant_t &heads);
	int32_t produceProtobufWithWaitResult(const variant_t &topicName, const variant_t &partition, const variant_t &key, const variant_t &heads);

	struct KafkaSettings {
		std::string Key;
		std::string Value;
	};

		// RAII class для управления Admin API ресурсами
		class AdminClientScope {
		public:
			AdminClientScope(SimpleKafka1C* owner,
			                 const std::string& brokers,
			                 const std::vector<KafkaSettings>& settings,
			                 rd_kafka_type_t type = RD_KAFKA_PRODUCER);
			~AdminClientScope();

		bool isValid() const { return rk != nullptr; }
		rd_kafka_t* get() const { return rk; }
		rd_kafka_queue_t* queue() const { return rkqu; }
		const std::string& error() const { return errstr_msg; }

		private:
			SimpleKafka1C* owner = nullptr;
			rd_kafka_t* rk = nullptr;
			rd_kafka_queue_t* rkqu = nullptr;
			std::string errstr_msg;
		};

	class clEventCb : public RdKafka::EventCb
	{
	public:
		unsigned pid;
		std::string formatLogFiles;
		std::string logDir;
		std::string consumerLogName;
		std::string statLogName;
		bool statisticsOn = false;
		std::string clientid;

		void event_cb(RdKafka::Event &event);
	};

	class clDeliveryReportCb : public RdKafka::DeliveryReportCb
	{
	public:
		unsigned pid;
		int32_t delivered;
		std::string formatLogFiles;
		std::string logDir;
		std::string producerLogName;
		std::string clientid;

		void dr_cb(RdKafka::Message &message);
	};

	class clRebalanceCb : public RdKafka::RebalanceCb
	{
	public:

		std::vector<RdKafka::TopicPartition*> offsets;

		void rebalance_cb(RdKafka::KafkaConsumer* consumer,
			RdKafka::ErrorCode err,
			std::vector<RdKafka::TopicPartition *> &partitions);
	};

		std::vector<KafkaSettings> settings;

#ifdef SIMPLEKAFKA_HAS_OPENSSL
		std::vector<std::string> loadedSslProviders;
		std::vector<OSSL_PROVIDER*> loadedSslProviderHandles;
		mutable std::mutex sslRuntimeMutex;
#endif

		clEventCb cl_event_cb;
	clDeliveryReportCb cl_dr_cb;
	clRebalanceCb cl_rebalance_cb;
};

// internal class from avro-cpp (Stream.cc)
class MemoryOutputStream : public avro::OutputStream
{
public:
	const size_t chunkSize_;
	std::vector<uint8_t *> data_;
	size_t available_;
	size_t byteCount_;

	explicit MemoryOutputStream(size_t chunkSize) : chunkSize_(chunkSize),
		available_(0), byteCount_(0) {}

	~MemoryOutputStream() final
	{
		for (std::vector<uint8_t *>::const_iterator it = data_.begin(); it != data_.end(); ++it)
			delete[] * it;
	}

	bool next(uint8_t **data, size_t *len) final
	{
		if (available_ == 0)
		{
			data_.push_back(new uint8_t[chunkSize_]);
			available_ = chunkSize_;
		}
		*data = &data_.back()[chunkSize_ - available_];
		*len = available_;
		byteCount_ += available_;
		available_ = 0;
		return true;
	}

	void backup(size_t len) final
	{
		available_ += len;
		byteCount_ -= len;
	}

	uint64_t byteCount() const final
	{
		return byteCount_;
	}

	void flush() final {}

	void snapshot(std::vector<uint8_t> &result)
	{
		size_t c = byteCount_;
		result.reserve(byteCount_);
		for (auto it = data_.begin(); it != data_.end(); ++it)
		{
			const size_t n = (std::min)(c, chunkSize_);
			std::copy(*it, *it + n, std::back_inserter(result));
			c -= n;
		}
	}
};

// internal class for reading from memory
class MemoryInputStream : public avro::InputStream
{
public:
	const uint8_t* data_;
	size_t size_;
	size_t position_;

	MemoryInputStream(const uint8_t* data, size_t size) : data_(data), size_(size), position_(0) {}

	bool next(const uint8_t** data, size_t* len) final
	{
		if (position_ >= size_)
		{
			return false;
		}
		*data = data_ + position_;
		*len = size_ - position_;
		position_ = size_;
		return true;
	}

	void backup(size_t len) final
	{
		position_ -= len;
	}

	void skip(size_t len) final
	{
		position_ += len;
		if (position_ > size_)
		{
			position_ = size_;
		}
	}

	size_t byteCount() const final
	{
		return position_;
	}
};

#endif //SIMPLEKAFKA1C_H
