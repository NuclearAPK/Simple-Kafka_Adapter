/*
 *  Simple Kafka 1C - Admin Metadata Methods
 *  Cluster and Broker information, Topic metadata, Partition watermarks
 */

#include "SimpleKafka1C.h"
#include "utils.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <sstream>

//================================== Cluster and Broker Information ==========================================

std::string SimpleKafka1C::getClusterInfo(const variant_t& brokers)
{
	std::string result;
	std::stringstream s{};

	std::string tBrokers = std::get<std::string>(brokers);

	// создаем конфигурацию
	RdKafkaConfPtr conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

	if (!applyKafkaSettings(conf.get(), result))
	{
		msg_err = result;
		return "";
	}

	if (conf->set("bootstrap.servers", tBrokers, result) != RdKafka::Conf::CONF_OK)
	{
		msg_err = result;
		return "";
	}

	// создаем продюсера для получения метаданных
	std::unique_ptr<RdKafka::Producer> producer(RdKafka::Producer::create(conf.get(), result));
	if (!producer)
	{
		msg_err = enrichSslError(std::string(u8"Ошибка создания клиента: ") + result);
		return "";
	}

	// получаем метаданные кластера
	RdKafka::Metadata* metadata = nullptr;
	RdKafka::ErrorCode err = producer->metadata(true, nullptr, &metadata, 5000);

	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(err);
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
	RdKafkaConfPtr conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

	if (!applyKafkaSettings(conf.get(), result))
	{
		msg_err = result;
		return "";
	}

	if (conf->set("bootstrap.servers", tBrokers, result) != RdKafka::Conf::CONF_OK)
	{
		msg_err = result;
		return "";
	}

	// создаем продюсера для получения метаданных
	std::unique_ptr<RdKafka::Producer> producer(RdKafka::Producer::create(conf.get(), result));
	if (!producer)
	{
		msg_err = enrichSslError(std::string(u8"Ошибка создания клиента: ") + result);
		return "";
	}

	// получаем метаданные кластера
	RdKafka::Metadata* metadata = nullptr;
	RdKafka::ErrorCode err = producer->metadata(true, nullptr, &metadata, 5000);

	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(err);
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

	if (!found)
	{
		msg_err = u8"Брокер с ID " + std::to_string(tBrokerId) + u8" не найден";
		return "";
	}

	boost::property_tree::write_json(s, jsonObj, true);
	return s.str();
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
		msg_err = enrichSslError(std::string(u8"Ошибка создания временного продюсера: ") + msg_err);
		return result;
	}

	// получаем метаданные для конкретного топика
	std::unique_ptr<RdKafka::Topic> topicHandle(RdKafka::Topic::create(producer.get(), tTopicName, nullptr, msg_err));
	if (!topicHandle)
	{
		msg_err = u8"Ошибка создания дескриптора топика: " + msg_err;
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
	std::string result;
	std::stringstream s{};

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);
	int32_t tPartition = std::get<int32_t>(partition);

	// создаем конфигурацию
	RdKafkaConfPtr conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

	if (!applyKafkaSettings(conf.get(), result))
	{
		msg_err = result;
		return "";
	}

	if (conf->set("bootstrap.servers", tBrokers, result) != RdKafka::Conf::CONF_OK)
	{
		msg_err = result;
		return "";
	}

	// запрещаем автосоздание топиков
	if (conf->set("allow.auto.create.topics", "false", result) != RdKafka::Conf::CONF_OK)
	{
		msg_err = result;
		return "";
	}

	// создаем продюсера для получения watermarks
	std::unique_ptr<RdKafka::Producer> producer(RdKafka::Producer::create(conf.get(), result));
	if (!producer)
	{
		msg_err = enrichSslError(std::string(u8"Ошибка создания клиента: ") + result);
		return "";
	}

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
	std::string result;
	std::string tBrokers = std::get<std::string>(brokers);
	int32_t tTimeout = std::get<int32_t>(timeout);

	// создаем конфигурацию
	RdKafkaConfPtr conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

	if (!applyKafkaSettings(conf.get(), result))
	{
		msg_err = result;
		return false;
	}

	if (conf->set("bootstrap.servers", tBrokers, result) != RdKafka::Conf::CONF_OK)
	{
		msg_err = result;
		return false;
	}

	// создаем продюсера для проверки подключения
	std::unique_ptr<RdKafka::Producer> producer(RdKafka::Producer::create(conf.get(), result));
	if (!producer)
	{
		msg_err = enrichSslError(std::string(u8"Ошибка подключения к брокеру: ") + result);
		return false;
	}

	// Пытаемся получить метаданные - это проверит доступность брокера
	RdKafka::Metadata* metadata = nullptr;
	RdKafka::ErrorCode err = producer->metadata(true, nullptr, &metadata, tTimeout);

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
	RdKafkaConfPtr conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

	if (!applyKafkaSettings(conf.get(), result))
	{
		msg_err = result;
		return -1.0;
	}

	if (conf->set("bootstrap.servers", tBrokers, result) != RdKafka::Conf::CONF_OK)
	{
		msg_err = result;
		return -1.0;
	}

	// запрещаем автосоздание топиков
	if (conf->set("allow.auto.create.topics", "false", result) != RdKafka::Conf::CONF_OK)
	{
		msg_err = result;
		return -1.0;
	}

	// создаем продюсера для получения watermarks
	std::unique_ptr<RdKafka::Producer> producer(RdKafka::Producer::create(conf.get(), result));
	if (!producer)
	{
		msg_err = enrichSslError(std::string(u8"Ошибка создания клиента: ") + result);
		return -1.0;
	}

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
