#ifndef KAFKA_UTILS_H
#define KAFKA_UTILS_H

#include <string>
#include <cstddef>
#include <cstdint>

// String utilities
char* slice(char* s, size_t from, size_t to);
std::string base64Encode(const uint8_t* data, size_t len);
bool isValidUtf8(const char* data, size_t len);

// Date/time utilities
std::string currentDateTime();
std::string currentDateTime(const char* format);
intmax_t getTimeStamp();

//================================== Input Validation ==========================================

// URL validation for Schema Registry
bool isValidUrl(const std::string& url);

// JSON validation
bool isValidJson(const std::string& json, std::string& errorMsg);

// Topic name validation (Kafka topic naming rules)
bool isValidTopicName(const std::string& topicName, std::string& errorMsg);

// Broker address validation (host:port format)
bool isValidBrokerAddress(const std::string& address, std::string& errorMsg);

// Broker list validation (comma-separated list of host:port)
bool isValidBrokerList(const std::string& brokerList, std::string& errorMsg);

// Partition number validation
bool isValidPartition(int32_t partition, std::string& errorMsg);

// Replication factor validation
bool isValidReplicationFactor(int32_t replicationFactor, std::string& errorMsg);

// Consumer group ID validation
bool isValidConsumerGroupId(const std::string& groupId, std::string& errorMsg);

#endif // KAFKA_UTILS_H
