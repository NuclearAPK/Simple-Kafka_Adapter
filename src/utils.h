#ifndef KAFKA_UTILS_H
#define KAFKA_UTILS_H

#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

#include "Component.h"

// String utilities
std::string base64Encode(const uint8_t* data, size_t len);
// Decode a strictly-valid base64 string into raw bytes. Returns false (and
// leaves out untouched) if the input is not pure base64: a character outside
// the alphabet, data after the padding, more than two '=', or padding that
// does not complete the final group to a multiple of four.
// Tolerates both alphabets ('+/' and URL-safe '-_'), fully omitted padding,
// and whitespace/line breaks anywhere in the input.
bool tryBase64Decode(const std::string& input, std::vector<char>& out);
bool isValidUtf8(const char* data, size_t len);

// Number utilities

// Converts a number coming from 1C into int64. Accepts int32 and double, since
// 1C marshals values that do not fit into int32 as VTYPE_R8 (double) - variant_t
// has no int64 alternative. Rejects non-numbers, NaN/infinity, fractional values
// and magnitudes above 2^53, where double stops representing consecutive integers
// exactly. Never throws: on failure returns false, fills errorMsg and leaves out
// untouched.
bool variantToInt64(const variant_t& value, int64_t& out, std::string& errorMsg);

// Converts a millisecond Unix timestamp coming from 1C into int64. Accepts a
// String (parsed as a whole number, the only form that always survives the
// transfer intact) and a double. Rejects int32: no real millisecond timestamp
// fits into int32, so such a value means the number was narrowed on its way
// from 1C and cannot be restored. Rejects negative values. Never throws: on
// failure returns false, fills errorMsg and leaves out untouched.
bool variantToTimestampMs(const variant_t& value, int64_t& out, std::string& errorMsg);

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
