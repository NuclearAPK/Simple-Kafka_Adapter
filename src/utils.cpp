#include "utils.h"

#include <chrono>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <regex>
#include <boost/json.hpp>

std::string base64Encode(const uint8_t* data, size_t len)
{
	static constexpr char table[] =
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

	std::string out;
	out.reserve(((len + 2) / 3) * 4);

	for (size_t i = 0; i < len; i += 3)
	{
		const uint32_t octetA = data[i];
		const uint32_t octetB = (i + 1 < len) ? data[i + 1] : 0;
		const uint32_t octetC = (i + 2 < len) ? data[i + 2] : 0;
		const uint32_t triple = (octetA << 16) | (octetB << 8) | octetC;

		out.push_back(table[(triple >> 18) & 0x3F]);
		out.push_back(table[(triple >> 12) & 0x3F]);
		out.push_back((i + 1 < len) ? table[(triple >> 6) & 0x3F] : '=');
		out.push_back((i + 2 < len) ? table[triple & 0x3F] : '=');
	}

	return out;
}

bool tryBase64Decode(const std::string& input, std::vector<char>& out)
{
	auto val = [](unsigned char c) -> int {
		if (c >= 'A' && c <= 'Z') return c - 'A';
		if (c >= 'a' && c <= 'z') return c - 'a' + 26;
		if (c >= '0' && c <= '9') return c - '0' + 52;
		if (c == '+' || c == '-') return 62; // '-' = URL-safe
		if (c == '/' || c == '_') return 63; // '_' = URL-safe
		return -1;
	};

	std::vector<int> sextets;
	sextets.reserve(input.size());
	for (unsigned char c : input)
	{
		if (c == '=') break; // padding -> end of meaningful data
		if (c == ' ' || c == '\t' || c == '\r' || c == '\n')
			continue; // skip whitespace / line breaks
		const int v = val(c);
		if (v < 0)
			return false; // not valid base64
		sextets.push_back(v);
	}

	const size_t k = sextets.size();
	if (k == 0 || (k % 4) == 1)
		return false;

	std::vector<char> result;
	result.reserve((k / 4) * 3 + 2);

	size_t i = 0;
	for (; i + 4 <= k; i += 4)
	{
		const uint32_t t = (static_cast<uint32_t>(sextets[i]) << 18) |
		                   (static_cast<uint32_t>(sextets[i + 1]) << 12) |
		                   (static_cast<uint32_t>(sextets[i + 2]) << 6) |
		                   static_cast<uint32_t>(sextets[i + 3]);
		result.push_back(static_cast<char>((t >> 16) & 0xFF));
		result.push_back(static_cast<char>((t >> 8) & 0xFF));
		result.push_back(static_cast<char>(t & 0xFF));
	}

	const size_t rem = k - i;
	if (rem == 2) // final group encodes 1 byte
	{
		const uint32_t t = (static_cast<uint32_t>(sextets[i]) << 18) |
		                   (static_cast<uint32_t>(sextets[i + 1]) << 12);
		result.push_back(static_cast<char>((t >> 16) & 0xFF));
	}
	else if (rem == 3) // final group encodes 2 bytes
	{
		const uint32_t t = (static_cast<uint32_t>(sextets[i]) << 18) |
		                   (static_cast<uint32_t>(sextets[i + 1]) << 12) |
		                   (static_cast<uint32_t>(sextets[i + 2]) << 6);
		result.push_back(static_cast<char>((t >> 16) & 0xFF));
		result.push_back(static_cast<char>((t >> 8) & 0xFF));
	}

	out = std::move(result);
	return true;
}

bool isValidUtf8(const char* data, size_t len)
{
	size_t i = 0;
	while (i < len)
	{
		const unsigned char c = static_cast<unsigned char>(data[i]);
		size_t remaining = 0;

		if ((c & 0x80) == 0x00)
		{
			i++;
			continue;
		}
		else if ((c & 0xE0) == 0xC0)
		{
			remaining = 1;
			if (c < 0xC2) return false; // overlong
		}
		else if ((c & 0xF0) == 0xE0)
		{
			remaining = 2;
		}
		else if ((c & 0xF8) == 0xF0)
		{
			remaining = 3;
			if (c > 0xF4) return false; // > U+10FFFF
		}
		else
		{
			return false;
		}

		if (i + remaining >= len) return false;

		for (size_t j = 1; j <= remaining; ++j)
		{
			const unsigned char cc = static_cast<unsigned char>(data[i + j]);
			if ((cc & 0xC0) != 0x80) return false;
		}

		// Extra checks for overlong/surrogates
		if (remaining == 2)
		{
			const unsigned char c1 = static_cast<unsigned char>(data[i + 1]);
			if (c == 0xE0 && c1 < 0xA0) return false; // overlong
			if (c == 0xED && c1 >= 0xA0) return false; // surrogate
		}
		else if (remaining == 3)
		{
			const unsigned char c1 = static_cast<unsigned char>(data[i + 1]);
			if (c == 0xF0 && c1 < 0x90) return false; // overlong
			if (c == 0xF4 && c1 >= 0x90) return false; // > U+10FFFF
		}

		i += remaining + 1;
	}

	return true;
}

std::string currentDateTime()
{
	std::chrono::time_point now = std::chrono::high_resolution_clock::now();
	tm current{};

#ifdef _WIN32
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

#ifdef _WIN32
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
