#ifndef SIMPLEKAFKA1C_H
#define SIMPLEKAFKA1C_H

#include <librdkafka/rdkafkacpp.h>
#include <avro/ValidSchema.hh>
#include <avro/Stream.hh>
#include "Component.h"

class SimpleKafka1C final : public Component
{
public:
	static constexpr char Version[] = u8"1.5.1";

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

	std::vector<HeadersMessage> messageHeaders;

	// avro
	std::map<std::string, avro::ValidSchema> schemesMap;	// кеш для хранение компилированных схем Avro
	std::vector<uint8_t> avroFile;		// формируемый avro

	// parameters set 
	// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	void setParameter(const variant_t &key, const variant_t &value);

	std::string clientID();
	std::string extensionName() override;

	// producer
	bool initProducer(const variant_t &brokers);
	int32_t produce(const variant_t &msg, const variant_t &topicName, const variant_t &partition, const variant_t &key, const variant_t &heads);
	int32_t produceWithWaitResult(const variant_t &msg, const variant_t &topicName, const variant_t &partition, const variant_t &key, const variant_t &heads);
	int32_t produceAvro(const variant_t &topicName, const variant_t &partition, const variant_t &key, const variant_t &heads);
	int32_t produceAvroWithWaitResult(const variant_t &topicName, const variant_t &partition, const variant_t &key, const variant_t &heads);
	bool stopProducer();

	// consumer
	void clearMessageMetadata(); 
	bool initConsumer(const variant_t& brokers);
	bool subscribe(const variant_t& topic);
	std::string consume();	// устарела. рекомендуется использовать getMessage + getMessageMetadata + getMessageData
	bool getMessage();	// чтение с подтверждением
	variant_t getMessageData(const variant_t &binaryResult);	// данные, как они есть в kafka
	std::string getMessageKey();
	std::string getMessageHeaders();
	int32_t getMessageOffset();
	std::string getMessageTopicName();
	int32_t getMessageBrokerID();
	int32_t getMessageTimestamp();
	int32_t getMessagePartition();

	bool commitOffset(const variant_t &topicName, const variant_t &offset, const variant_t &partition);
	bool setReadingPosition(const variant_t &topicName, const variant_t &offset, const variant_t &partition);
	bool setReadingPositions(const variant_t& jsonTopicPartitions);
	bool stopConsumer();
	bool setWaitingTimeout(const variant_t &timeout);

	// admin
	std::string getListOfTopics(const variant_t& brokers);
	std::string getTopicOptions(const variant_t& topicName); // experemental
	std::string getConsumerCurrentGroupOffset(const variant_t& times, const variant_t& timeout);
	std::string getConsumerGroupOffsets(const variant_t& brokers, const variant_t& times, const variant_t& timeout);
	bool createTopic(const variant_t& brokers, const variant_t& topicName, const variant_t& partition, const variant_t& replication_factor);

	// Utilites
	bool sleep(const variant_t &delay);
	bool setLogDirectory(const variant_t& logDir);
	bool setFormatLogFiles(const variant_t& format);
	std::string getLastError() { return msg_err; }
    void openEventFile(const std::string& logName, std::ofstream& eventFile);

	// converting a message to avro format
	bool putAvroSchema(const variant_t &schemaJsonName, const variant_t &schemaJson);
	bool convertToAvroFormat(const variant_t &msgJson, const variant_t &schemaJsonName);
	bool saveAvroFile(const variant_t &fileName);

	struct KafkaSettings {
		std::string Key;
		std::string Value;
	};

	class clEventCb : public RdKafka::EventCb
	{
	public:
		unsigned pid;
		char *formatLogFiles;
		std::string logDir = "";
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
		char *formatLogFiles;
		std::string logDir = "";
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
		for (std::vector<uint8_t *>::const_iterator it = data_.begin();
			it != data_.end(); ++it)
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
			#if defined( __linux__ )
			const size_t n = std::min(c, chunkSize_);
			#else
			const size_t n = min(c, chunkSize_);
			#endif
			std::copy(*it, *it + n, std::back_inserter(result));
			c -= n;
		}
	}
};

#endif //SIMPLEKAFKA1C_H
