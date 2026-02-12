#include "utils.h"

#include <chrono>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <regex>
#include <boost/json.hpp>

char* slice(char* s, size_t from, size_t to)
{
	size_t j = 0;
	for (size_t i = from; i <= to; ++i)
		s[j++] = s[i];
	s[j] = 0;
	return s;
}

std::string currentDateTime()
{
	std::chrono::time_point now = std::chrono::high_resolution_clock::now();
	tm current{};

#ifdef _WINDOWS
	time_t time = std::time(nullptr);
	localtime_s(&current, &time);
#else
	auto time = std::chrono::system_clock::to_time_t(now);
	gmtime_r(&time, &current);
#endif

	auto epoch = now.time_since_epoch();
	auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(epoch).count() % 1000000000;

	std::ostringstream oss{};
	oss << std::put_time(&current, "%Y-%m-%d %T.") << ns;
	return oss.str();
}

std::string currentDateTime(const char* format)
{
	std::chrono::time_point now = std::chrono::high_resolution_clock::now();
	tm current{};

#ifdef _WINDOWS
	time_t time = std::time(nullptr);
	localtime_s(&current, &time);
#else
	auto time = std::chrono::system_clock::to_time_t(now);
	gmtime_r(&time, &current);
#endif

	std::ostringstream oss{};
	oss << std::put_time(&current, format);
	return oss.str();
}

intmax_t getTimeStamp()
{
	time_t curtime = time(nullptr);
	intmax_t timestamp = static_cast<intmax_t>(curtime);
	return timestamp;
}

//================================== Input Validation ==========================================

bool isValidUrl(const std::string& url)
{
	if (url.empty())
	{
		return false;
	}

	// Check for valid URL scheme (http:// or https://)
	static const std::regex urlPattern(R"(
		^(https?):
		//
		(
		  ([a-zA-Z0-9]([a-zA-Z0-9\-._~]*[a-zA-Z0-9])?)  
		  |(\[[0-9a-fA-F:]+\])                          
		  |((([1-9]?\d|1\d\d|2[0-4]\d|25[0-5])\.){3}    
			 ([1-9]?\d|1\d\d|2[0-4]\d|25[0-5]))
		)
		(:(\d{1,5}))?                                    
		(/[^\s]*)?                                       
		$)", std::regex::extended | std::regex::icase);

	return std::regex_match(url, urlPattern);
}

bool isValidJson(const std::string& json, std::string& errorMsg)
{
	if (json.empty())
	{
		errorMsg = "JSON string is empty";
		return false;
	}

	try
	{
		boost::json::parse(json);
		return true;
	}
	catch (const std::exception& e)
	{
		errorMsg = std::string("Invalid JSON: ") + e.what();
		return false;
	}
}

bool isValidTopicName(const std::string& topicName, std::string& errorMsg)
{
	if (topicName.empty())
	{
		errorMsg = "Topic name cannot be empty";
		return false;
	}

	// Kafka topic name rules:
	// - Max 249 characters
	// - Allowed characters: a-z, A-Z, 0-9, '.', '_', '-'
	// - Cannot be "." or ".."
	if (topicName.length() > 249)
	{
		errorMsg = "Topic name exceeds maximum length of 249 characters";
		return false;
	}

	if (topicName == "." || topicName == "..")
	{
		errorMsg = "Topic name cannot be '.' or '..'";
		return false;
	}

	static const std::regex topicPattern("^[a-zA-Z0-9._-]+$");
	if (!std::regex_match(topicName, topicPattern))
	{
		errorMsg = "Topic name contains invalid characters. Allowed: a-z, A-Z, 0-9, '.', '_', '-'";
		return false;
	}

	return true;
}

bool isValidBrokerAddress(const std::string& address, std::string& errorMsg)
{
	if (address.empty())
	{
		errorMsg = "Broker address cannot be empty";
		return false;
	}

	// Format: host:port or host (port optional)
	// host can be hostname, IPv4, or IPv6 (in brackets)
	static const std::regex brokerPattern(
		R"(^([a-zA-Z0-9\-._]+|\[[:0-9a-fA-F]+\])(:(\d{1,5}))?$)"
	);

	std::smatch match;
	if (!std::regex_match(address, match, brokerPattern))
	{
		errorMsg = "Invalid broker address format. Expected: host:port or host";
		return false;
	}

	// Validate port if present
	if (match[3].matched)
	{
		int port = std::stoi(match[3].str());
		if (port < 1 || port > 65535)
		{
			errorMsg = "Port must be between 1 and 65535";
			return false;
		}
	}

	return true;
}

bool isValidBrokerList(const std::string& brokerList, std::string& errorMsg)
{
	if (brokerList.empty())
	{
		errorMsg = "Broker list cannot be empty";
		return false;
	}

	// Split by comma and validate each broker
	std::stringstream ss(brokerList);
	std::string broker;
	int count = 0;

	while (std::getline(ss, broker, ','))
	{
		// Trim whitespace
		size_t start = broker.find_first_not_of(" \t");
		size_t end = broker.find_last_not_of(" \t");

		if (start == std::string::npos)
		{
			errorMsg = "Empty broker address in list";
			return false;
		}

		broker = broker.substr(start, end - start + 1);

		if (!isValidBrokerAddress(broker, errorMsg))
		{
			return false;
		}
		count++;
	}

	if (count == 0)
	{
		errorMsg = "Broker list is empty";
		return false;
	}

	return true;
}

bool isValidPartition(int32_t partition, std::string& errorMsg)
{
	if (partition < -1)  // -1 is valid for "any partition"
	{
		errorMsg = "Partition must be >= -1 (use -1 for automatic assignment)";
		return false;
	}

	// Kafka supports up to 2^31-1 partitions, but practically much less
	if (partition > 10000)
	{
		errorMsg = "Warning: partition number is unusually high (> 10000)";
		// Still return true as it's technically valid
	}

	return true;
}

bool isValidReplicationFactor(int32_t replicationFactor, std::string& errorMsg)
{
	if (replicationFactor < 1)
	{
		errorMsg = "Replication factor must be >= 1";
		return false;
	}

	if (replicationFactor > 32767)
	{
		errorMsg = "Replication factor exceeds maximum (32767)";
		return false;
	}

	return true;
}

bool isValidConsumerGroupId(const std::string& groupId, std::string& errorMsg)
{
	if (groupId.empty())
	{
		errorMsg = "Consumer group ID cannot be empty";
		return false;
	}

	// Max 255 characters
	if (groupId.length() > 255)
	{
		errorMsg = "Consumer group ID exceeds maximum length of 255 characters";
		return false;
	}

	// Allowed characters similar to topic names
	static const std::regex groupIdPattern("^[a-zA-Z0-9._-]+$");
	if (!std::regex_match(groupId, groupIdPattern))
	{
		errorMsg = "Consumer group ID contains invalid characters. Allowed: a-z, A-Z, 0-9, '.', '_', '-'";
		return false;
	}

	return true;
}
