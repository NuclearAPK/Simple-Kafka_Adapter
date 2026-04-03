/*
 *  Simple Kafka 1C - Admin Metadata Methods
 *  Cluster and Broker information, Topic metadata, Partition watermarks
 */

#include "SimpleKafka1C.h"
#include "utils.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <map>
#include <sstream>

//================================== Helper ==========================================

std::unique_ptr<RdKafka::Producer> SimpleKafka1C::createMetadataClient(const std::string& brokers)
{
	std::string errstr;
	RdKafkaConfPtr conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

	if (!applyKafkaSettings(conf.get(), errstr))
	{
		msg_err = errstr;
		return nullptr;
	}

	if (conf->set("bootstrap.servers", brokers, errstr) != RdKafka::Conf::CONF_OK)
	{
		msg_err = errstr;
		return nullptr;
	}

	std::unique_ptr<RdKafka::Producer> producer(RdKafka::Producer::create(conf.get(), errstr));
	if (!producer)
	{
		msg_err = enrichSslError(std::string("Client creation error: ") + errstr);
		return nullptr;
	}

	return producer;
}

//================================== Cluster and Broker Information ==========================================

std::string SimpleKafka1C::getClusterInfo(const variant_t& brokers)
{
	std::stringstream s{};

	auto producer = createMetadataClient(std::get<std::string>(brokers));
	if (!producer)
		return "";

	// получаем метаданные кластера
	RdKafka::Metadata* metadata = nullptr;
	RdKafka::ErrorCode err = producer->metadata(true, nullptr, &metadata, 5000);

	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(err);
		return "";
	}

	try
	{
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

		boost::property_tree::write_json(s, jsonObj, true);
		return s.str();
	}
	catch (const std::exception& e)
	{
		msg_err = std::string("Exception in getClusterInfo: ") + e.what();
		delete metadata;
		return "";
	}
}

std::string SimpleKafka1C::getBrokerInfo(const variant_t& brokers, const variant_t& brokerId)
{
	std::stringstream s{};
	int32_t tBrokerId = std::get<int32_t>(brokerId);

	auto producer = createMetadataClient(std::get<std::string>(brokers));
	if (!producer)
		return "";

	// получаем метаданные кластера
	RdKafka::Metadata* metadata = nullptr;
	RdKafka::ErrorCode err = producer->metadata(true, nullptr, &metadata, 5000);

	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(err);
		return "";
	}

	try
	{
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

		if (!found)
		{
			msg_err = "Broker with ID " + std::to_string(tBrokerId) + " not found";
			return "";
		}

		boost::property_tree::write_json(s, jsonObj, true);
		return s.str();
	}
	catch (const std::exception& e)
	{
		msg_err = std::string("Exception in getBrokerInfo: ") + e.what();
		delete metadata;
		return "";
	}
}

std::string SimpleKafka1C::getTopicMetadata(const variant_t& brokers, const variant_t& topicName, const variant_t& timeout)
{
	std::string result;
	std::stringstream s{};

	boost::property_tree::ptree jsonObj;
	RdKafkaConfPtr conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);
	int32_t tTimeout = std::get<int32_t>(timeout);

	if (!applyKafkaSettings(conf.get(), msg_err))
	{
		return result;
	}

	if (conf->set("metadata.broker.list", tBrokers, msg_err) != RdKafka::Conf::CONF_OK)
	{
		return result;
	}

	// запрещаем автосоздание топиков при запросе метаданных
	if (conf->set("allow.auto.create.topics", "false", msg_err) != RdKafka::Conf::CONF_OK)
	{
		return result;
	}

	// создаем временного продюсера для получения метаданных
	std::unique_ptr<RdKafka::Producer> producer(RdKafka::Producer::create(conf.get(), msg_err));
	if (!producer)
	{
		msg_err = enrichSslError(std::string("Client creation error: ") + msg_err);
		return result;
	}

	// получаем метаданные для конкретного топика
	std::unique_ptr<RdKafka::Topic> topicHandle(RdKafka::Topic::create(producer.get(), tTopicName, nullptr, msg_err));
	if (!topicHandle)
	{
		msg_err = "Topic handle creation error: " + msg_err;
		return result;
	}

	class RdKafka::Metadata* metadata;
	RdKafka::ErrorCode err = producer->metadata(false, topicHandle.get(), &metadata, tTimeout);
	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(err);
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

	boost::property_tree::write_json(s, jsonObj, true);
	return s.str();
}

std::string SimpleKafka1C::getPartitionWatermarks(const variant_t& brokers,
                                                   const variant_t& topicName,
                                                   const variant_t& partition)
{
	std::stringstream s{};

	std::string tTopicName = std::get<std::string>(topicName);
	int32_t tPartition = std::get<int32_t>(partition);

	auto producer = createMetadataClient(std::get<std::string>(brokers));
	if (!producer)
		return "";

	// получаем watermarks для партиции
	int64_t low = 0;
	int64_t high = 0;
	RdKafka::ErrorCode err = producer->query_watermark_offsets(tTopicName, tPartition, &low, &high, 5000);

	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(err);
		return "";
	}

	// формируем JSON результат
	boost::property_tree::ptree jsonObj;
	jsonObj.put("topic", tTopicName);
	jsonObj.put("partition", std::to_string(tPartition));
	jsonObj.put("low_watermark", std::to_string(low));
	jsonObj.put("high_watermark", std::to_string(high));
	jsonObj.put("message_count", std::to_string(high - low));

	boost::property_tree::write_json(s, jsonObj, true);
	return s.str();
}

bool SimpleKafka1C::pingBroker(const variant_t& brokers, const variant_t& timeout)
{
	int32_t tTimeout = std::get<int32_t>(timeout);

	auto producer = createMetadataClient(std::get<std::string>(brokers));
	if (!producer)
		return false;

	// Пытаемся получить метаданные - это проверит доступность брокера
	RdKafka::Metadata* metadata = nullptr;
	RdKafka::ErrorCode err = producer->metadata(true, nullptr, &metadata, tTimeout);

	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = "Broker unavailable: " + std::string(RdKafka::err2str(err));
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
	std::string tTopicName = std::get<std::string>(topicName);
	int32_t tPartition = std::get<int32_t>(partition);

	auto producer = createMetadataClient(std::get<std::string>(brokers));
	if (!producer)
		return -1.0;

	// получаем watermarks для партиции
	int64_t low = 0;
	int64_t high = 0;
	RdKafka::ErrorCode err = producer->query_watermark_offsets(tTopicName, tPartition, &low, &high, 5000);

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
	RdKafkaConfPtr conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
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

	boost::property_tree::write_json(s, jsonObj, true);
	return s.str();
}

std::string SimpleKafka1C::getTopicSize(const variant_t& brokers, const variant_t& topicName, const variant_t& timeout)
{
	std::string result;
	std::stringstream s{};

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);
	int32_t tTimeout = std::get<int32_t>(timeout);

	// Validate input
	if (!isValidBrokerList(tBrokers, msg_err))
	{
		return result;
	}
	if (!isValidTopicName(tTopicName, msg_err))
	{
		return result;
	}

	// Create a temporary producer to get metadata and watermarks
	auto producer = createMetadataClient(tBrokers);
	if (!producer)
		return result;

	// Get topic metadata to determine partitions
	std::unique_ptr<RdKafka::Topic> topicHandle(RdKafka::Topic::create(producer.get(), tTopicName, nullptr, msg_err));
	if (!topicHandle)
	{
		msg_err = "Topic handle creation error: " + msg_err;
		return result;
	}

	RdKafka::Metadata* metadata = nullptr;
	RdKafka::ErrorCode err = producer->metadata(false, topicHandle.get(), &metadata, tTimeout);
	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(err);
		return result;
	}

	// Find topic in metadata
	const RdKafka::Metadata::TopicMetadataVector* topics = metadata->topics();
	const RdKafka::TopicMetadata* topicMeta = nullptr;
	for (auto it = topics->begin(); it != topics->end(); ++it)
	{
		if ((*it)->topic() == tTopicName)
		{
			topicMeta = *it;
			break;
		}
	}

	if (!topicMeta)
	{
		msg_err = "Topic not found";
		delete metadata;
		return result;
	}

	int partition_cnt = static_cast<int>(topicMeta->partitions()->size());

	// Query watermarks for each partition
	struct PartitionInfo {
		int32_t id;
		int64_t low;
		int64_t high;
		int64_t size_bytes;
		int64_t message_count;
	};
	std::vector<PartitionInfo> partitions(partition_cnt);

	for (int i = 0; i < partition_cnt; i++)
	{
		partitions[i].id = (*topicMeta->partitions())[i]->id();
		partitions[i].size_bytes = 0;
		partitions[i].message_count = 0;

		err = producer->query_watermark_offsets(tTopicName, partitions[i].id,
		                                         &partitions[i].low, &partitions[i].high, tTimeout);
		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = "Failed to query watermarks for partition " + std::to_string(partitions[i].id)
			          + ": " + RdKafka::err2str(err);
			delete metadata;
			return result;
		}
	}

	delete metadata;

	// Check if topic has any messages
	bool hasMessages = false;
	for (const auto& p : partitions)
	{
		if (p.high > p.low)
		{
			hasMessages = true;
			break;
		}
	}

	if (!hasMessages)
	{
		// Topic is empty, return result without creating consumer
		boost::property_tree::ptree jsonObj;
		jsonObj.put("topic", tTopicName);
		jsonObj.put("total_size_bytes", "0");
		jsonObj.put("total_messages", "0");
		jsonObj.put("average_message_size", "0");
		jsonObj.put("partition_count", partition_cnt);

		boost::property_tree::ptree partitionsArray;
		for (const auto& p : partitions)
		{
			boost::property_tree::ptree partNode;
			partNode.put("partition", p.id);
			partNode.put("size_bytes", "0");
			partNode.put("message_count", "0");
			partNode.put("average_message_size", "0");
			partitionsArray.push_back(std::make_pair("", partNode));
		}
		jsonObj.put_child("partitions", partitionsArray);

		boost::property_tree::write_json(s, jsonObj, true);
		return s.str();
	}

	// Release producer before creating consumer
	producer.reset();

	// Create a temporary consumer to read all messages
	std::string errstr;
	RdKafkaConfPtr conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

	if (!applyKafkaSettings(conf.get(), errstr))
	{
		msg_err = errstr;
		return result;
	}

	if (conf->set("bootstrap.servers", tBrokers, errstr) != RdKafka::Conf::CONF_OK)
	{
		msg_err = errstr;
		return result;
	}

	// Use a unique group.id to avoid interfering with existing consumers
	std::string groupId = "simplekafka1c-topicsize-" + std::to_string(getTimeStamp()) + "-" + std::to_string(pid);
	if (conf->set("group.id", groupId, errstr) != RdKafka::Conf::CONF_OK)
	{
		msg_err = errstr;
		return result;
	}

	// Disable auto commit so we don't store offsets
	if (conf->set("enable.auto.commit", "false", errstr) != RdKafka::Conf::CONF_OK)
	{
		msg_err = errstr;
		return result;
	}

	std::unique_ptr<RdKafka::KafkaConsumer> consumer(RdKafka::KafkaConsumer::create(conf.get(), errstr));
	if (!consumer)
	{
		msg_err = enrichSslError(std::string("Consumer creation error: ") + errstr);
		return result;
	}

	// Assign all partitions starting from low watermark
	std::vector<RdKafka::TopicPartition*> assignPartitions;
	for (const auto& p : partitions)
	{
		if (p.high > p.low)
		{
			RdKafka::TopicPartition* tp = RdKafka::TopicPartition::create(tTopicName, p.id, p.low);
			assignPartitions.push_back(tp);
		}
	}

	err = consumer->assign(assignPartitions);
	for (auto* tp : assignPartitions)
		delete tp;

	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = "Failed to assign partitions: " + RdKafka::err2str(err);
		consumer->close();
		return result;
	}

	// Consume messages and calculate sizes
	auto startTime = std::chrono::steady_clock::now();
	int completedPartitions = 0;
	int totalPartitionsToRead = 0;

	// Track which partitions are done
	std::map<int32_t, bool> partitionDone;
	for (const auto& p : partitions)
	{
		if (p.high > p.low)
		{
			partitionDone[p.id] = false;
			totalPartitionsToRead++;
		}
		else
		{
			partitionDone[p.id] = true;
		}
	}

	while (completedPartitions < totalPartitionsToRead)
	{
		// Check timeout
		auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::steady_clock::now() - startTime).count();
		if (elapsed >= tTimeout)
		{
			msg_err = "Operation timed out after " + std::to_string(elapsed) + " ms";
			consumer->close();
			return result;
		}

		int remainingMs = static_cast<int>(tTimeout - elapsed);
		int pollTimeout = (std::min)(remainingMs, 1000);

		std::unique_ptr<RdKafka::Message> msg(consumer->consume(pollTimeout));

		if (!msg)
			continue;

		switch (msg->err())
		{
		case RdKafka::ERR_NO_ERROR:
		{
			int32_t partId = msg->partition();

			// Find partition in our vector and accumulate
			for (auto& p : partitions)
			{
				if (p.id == partId)
				{
					// Size = payload + key
					size_t msgSize = msg->len();
					if (msg->key_len() > 0)
						msgSize += msg->key_len();

					p.size_bytes += static_cast<int64_t>(msgSize);
					p.message_count++;

					// Check if this partition is done
					if (!partitionDone[partId] && (msg->offset() + 1) >= p.high)
					{
						partitionDone[partId] = true;
						completedPartitions++;
					}
					break;
				}
			}
			break;
		}
		case RdKafka::ERR__TIMED_OUT:
		case RdKafka::ERR__PARTITION_EOF:
			// Check if all partitions with messages have been fully read
			for (auto& p : partitions)
			{
				if (!partitionDone[p.id] && p.message_count >= (p.high - p.low))
				{
					partitionDone[p.id] = true;
					completedPartitions++;
				}
			}
			break;
		default:
			msg_err = "Consumer error: " + RdKafka::err2str(msg->err());
			consumer->close();
			return result;
		}
	}

	consumer->close();

	// Build JSON result
	boost::property_tree::ptree jsonObj;
	int64_t totalSize = 0;
	int64_t totalMessages = 0;

	boost::property_tree::ptree partitionsArray;
	for (const auto& p : partitions)
	{
		totalSize += p.size_bytes;
		totalMessages += p.message_count;

		boost::property_tree::ptree partNode;
		partNode.put("partition", p.id);
		partNode.put("size_bytes", std::to_string(p.size_bytes));
		partNode.put("message_count", std::to_string(p.message_count));
		double avgSize = (p.message_count > 0)
			? static_cast<double>(p.size_bytes) / static_cast<double>(p.message_count)
			: 0.0;
		partNode.put("average_message_size", std::to_string(static_cast<int64_t>(avgSize)));
		partitionsArray.push_back(std::make_pair("", partNode));
	}

	jsonObj.put("topic", tTopicName);
	jsonObj.put("total_size_bytes", std::to_string(totalSize));
	jsonObj.put("total_messages", std::to_string(totalMessages));
	double totalAvg = (totalMessages > 0)
		? static_cast<double>(totalSize) / static_cast<double>(totalMessages)
		: 0.0;
	jsonObj.put("average_message_size", std::to_string(static_cast<int64_t>(totalAvg)));
	jsonObj.put("partition_count", partition_cnt);
	jsonObj.put_child("partitions", partitionsArray);

	boost::property_tree::write_json(s, jsonObj, true);
	return s.str();
}
