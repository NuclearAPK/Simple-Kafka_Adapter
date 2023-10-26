#ifndef SIMPLEKAFKA1C_H
#define SIMPLEKAFKA1C_H

#include <librdkafka/rdkafkacpp.h>
#include <avro/ValidSchema.hh>
#include <avro/Stream.hh>
#include "Component.h"

//static std::string logsReportFileName;
//static bool delivered;

class SimpleKafka1C final : public Component
{
public:
	const char *Version = u8"1.3.1";

	SimpleKafka1C();
	~SimpleKafka1C();

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

	std::map<std::string, avro::ValidSchema> schemesMap;	// кеш для хранение компилированных схем Avro
	std::vector<uint8_t> avroFile;		// формируемый avro

	// parameters set 
	// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	void setParameter(const variant_t &key, const variant_t &value);
	std::string clientID();
	std::string extensionName();

	// producer
	bool initProducer(const variant_t &brokers);
	variant_t produce(const variant_t &msg, const variant_t &topicName, const variant_t &partition, const variant_t &key, const variant_t &heads);
	variant_t produceWithWaitResult(const variant_t &msg, const variant_t &topicName, const variant_t &partition, const variant_t &key, const variant_t &heads);
	variant_t produceDataFileToAvro(const variant_t &topicName, const variant_t &partition, const variant_t &key, const variant_t &heads);
	avro::ValidSchema getAvroSchema(const std::string &schemaJsonName, const std::string &schemaJson);
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

	// converting a message to avro format
	void putAvroSchema(const variant_t &schemaJsonName, const variant_t &schemaJson);
	void convertToAvroFormat(const variant_t &msgJson, const variant_t &schemaJsonName);
	void saveAvroFile(const variant_t &fileName);

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
		std::string clientid;

		void event_cb(RdKafka::Event &event);
	};

	class clDeliveryReportCb : public RdKafka::DeliveryReportCb
	{
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
