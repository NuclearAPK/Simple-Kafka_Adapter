#include "connect_failure.h"

#include <algorithm>
#include <cctype>

#include <librdkafka/rdkafka.h>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

namespace connect_failure
{

namespace
{
bool containsNoCase(const std::string& text, std::string_view needle)
{
	auto it = std::search(text.begin(), text.end(), needle.begin(), needle.end(),
		[](char a, char b) { return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b)); });
	return it != text.end();
}

std::string lowerTrim(std::string s)
{
	s.erase(0, s.find_first_not_of(" \t"));
	s.erase(s.find_last_not_of(" \t") + 1);
	std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
	return s;
}
} // namespace

FailureClass classifyKafkaError(int err, bool fatalFlag)
{
	if (fatalFlag)
		return FailureClass::Fatal;

	switch (static_cast<rd_kafka_resp_err_t>(err))
	{
	case RD_KAFKA_RESP_ERR_NO_ERROR:
		return FailureClass::None;
	case RD_KAFKA_RESP_ERR__SSL:
	case RD_KAFKA_RESP_ERR__AUTHENTICATION:
	case RD_KAFKA_RESP_ERR_SASL_AUTHENTICATION_FAILED:
	case RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED:
	case RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED:
	case RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED:
	case RD_KAFKA_RESP_ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
	case RD_KAFKA_RESP_ERR__FATAL:
		return FailureClass::Fatal;
	default:
		return FailureClass::Transient;
	}
}

namespace
{
bool isUtf8(const std::string& s)
{
	size_t i = 0;
	while (i < s.size())
	{
		const unsigned char c = static_cast<unsigned char>(s[i]);
		size_t n = 0;
		if (c < 0x80) { ++i; continue; }
		else if (c >= 0xC2 && c <= 0xDF) n = 1;
		else if (c >= 0xE0 && c <= 0xEF) n = 2;
		else if (c >= 0xF0 && c <= 0xF4) n = 3;
		else return false;
		if (i + n >= s.size()) return false;  // truncated sequence
		for (size_t j = 1; j <= n; ++j)
			if ((static_cast<unsigned char>(s[i + j]) & 0xC0) != 0x80)
				return false;
		i += n + 1;
	}
	return true;
}
} // namespace

std::string ensureUtf8(const std::string& text)
{
	if (isUtf8(text))
		return text;

#ifdef _WIN32
	const int wlen = MultiByteToWideChar(CP_ACP, 0, text.data(), static_cast<int>(text.size()), nullptr, 0);
	if (wlen > 0)
	{
		std::wstring wide(static_cast<size_t>(wlen), L'\0');
		MultiByteToWideChar(CP_ACP, 0, text.data(), static_cast<int>(text.size()), &wide[0], wlen);
		const int ulen = WideCharToMultiByte(CP_UTF8, 0, wide.data(), wlen, nullptr, 0, nullptr, nullptr);
		if (ulen > 0)
		{
			std::string utf8(static_cast<size_t>(ulen), '\0');
			WideCharToMultiByte(CP_UTF8, 0, wide.data(), wlen, &utf8[0], ulen, nullptr, nullptr);
			return utf8;
		}
	}
#endif

	std::string ascii(text);
	for (char& ch : ascii)
		if (static_cast<unsigned char>(ch) >= 0x80)
			ch = '?';
	return ascii;
}

bool isRetriableWaitError(int err)
{
	switch (static_cast<rd_kafka_resp_err_t>(err))
	{
	case RD_KAFKA_RESP_ERR__TIMED_OUT:
	case RD_KAFKA_RESP_ERR__TRANSPORT:
	case RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN:
	case RD_KAFKA_RESP_ERR__RESOLVE:
		return true;
	default:
		return false;
	}
}

std::string firstBrokerHost(const std::string& brokers)
{
	std::string first = brokers.substr(0, brokers.find(','));
	const auto scheme = first.find("://");
	if (scheme != std::string::npos)
		first = first.substr(scheme + 3);
	first.erase(0, first.find_first_not_of(" \t"));
	if (!first.empty() && first.front() == '[')  // IPv6 literal
	{
		const auto close = first.find(']');
		return close == std::string::npos ? first : first.substr(1, close - 1);
	}
	first = first.substr(0, first.find('/'));
	return first.substr(0, first.find(':'));
}

std::string connectHint(const std::string& errorText, const HintContext& ctx)
{
	const bool verifyFailed = containsNoCase(errorText, "certificate verify failed")
		|| containsNoCase(errorText, "error:0A000086")
		|| containsNoCase(errorText, "SSL peer certificate")       // curl
		|| containsNoCase(errorText, "SSL: no alternative certificate subject name")  // curl
		|| containsNoCase(errorText, "does not match target host name");               // curl

	if (verifyFailed && ctx.http)
	{
		std::string host = firstBrokerHost(ctx.brokers);
		return u8"Возможно, имя хоста " + (host.empty() ? std::string(u8"сервера") : host)
			+ u8" не совпадает с CN/SAN сертификата сервера, либо его CA не входит в доверенные сертификаты системы. "
			  u8"Перевыпустите сертификат с SAN для этого имени или добавьте CA в хранилище доверенных сертификатов";
	}

	if (verifyFailed)
	{
		if (lowerTrim(ctx.endpointIdentification) == "none")
			return u8"Проверьте ssl.ca.location: указанный CA не подтверждает сертификат брокера";

		std::string host = firstBrokerHost(ctx.brokers);
		return u8"Возможно, имя хоста " + (host.empty() ? std::string(u8"брокера") : host)
			+ u8" не совпадает с CN/SAN сертификата брокера, либо ssl.ca.location не подтверждает сертификат. "
			  u8"Перевыпустите сертификат брокера с SAN для этого имени или задайте "
			  u8"ssl.endpoint.identification.algorithm=none (отключает проверку имени хоста и ослабляет защиту)";
	}

	if (containsNoCase(errorText, "ssl.key.location") || containsNoCase(errorText, "private key")
		|| containsNoCase(errorText, "ssl.certificate.location") || containsNoCase(errorText, "client SSL authentication"))
		return u8"Проверьте ssl.key.location, ssl.certificate.location и ssl.key.password";

	if (containsNoCase(errorText, "SASL") || containsNoCase(errorText, "Authentication failed"))
		return u8"Проверьте sasl.mechanism, sasl.username и sasl.password";

	if (containsNoCase(errorText, "AUTHORIZATION_FAILED") || containsNoCase(errorText, "not authorized"))
		return u8"У учётной записи нет прав на операцию: проверьте ACL брокера";

	return {};
}

void ConnectFailureState::record(int err, const std::string& text, bool fatalFlag)
{
	const FailureClass cls = classifyKafkaError(err, fatalFlag);
	if (cls == FailureClass::None)
		return;

	const std::string utf8 = ensureUtf8(text);
	std::lock_guard<std::mutex> lk(mtx);
	last = utf8;
	if (cls == FailureClass::Fatal && !fatal)
	{
		fatal = true;
		firstFatal = utf8;
	}
}

void ConnectFailureState::recordFatalText(const std::string& text)
{
	const std::string utf8 = ensureUtf8(text);
	std::lock_guard<std::mutex> lk(mtx);
	last = utf8;
	if (!fatal)
	{
		fatal = true;
		firstFatal = utf8;
	}
}

bool ConnectFailureState::hasFatal() const
{
	std::lock_guard<std::mutex> lk(mtx);
	return fatal;
}

std::string ConnectFailureState::fatalText() const
{
	std::lock_guard<std::mutex> lk(mtx);
	return firstFatal;
}

std::string ConnectFailureState::lastText() const
{
	std::lock_guard<std::mutex> lk(mtx);
	return last;
}

void ConnectFailureState::reset()
{
	std::lock_guard<std::mutex> lk(mtx);
	fatal = false;
	firstFatal.clear();
	last.clear();
}

std::string formatConnectError(std::string_view operation, std::string_view baseError,
                               const ConnectFailureState& state, const HintContext& ctx)
{
	std::string result;
	if (!operation.empty())
	{
		result += operation;
		result += ": ";
	}

	if (state.hasFatal())
	{
		const std::string text = state.fatalText();
		result += text;
		const std::string hint = connectHint(text, ctx);
		if (!hint.empty())
			result += " | " + hint;
		return result;
	}

	result += baseError;
	const std::string last = state.lastText();
	if (!last.empty() && baseError.find(last) == std::string_view::npos)
	{
		result += " | " + last;
		const std::string hint = connectHint(last, ctx);
		if (!hint.empty())
			result += " | " + hint;
	}
	return result;
}

void rdkafkaErrorCallback(rd_kafka_t* rk, int err, const char* reason, void* opaque)
{
	auto* st = static_cast<ConnectFailureState*>(opaque);
	if (!st)
		return;
	if (err == RD_KAFKA_RESP_ERR__FATAL)
	{
		char buf[512] = {0};
		rd_kafka_fatal_error(rk, buf, sizeof(buf));
		st->record(err, buf, true);
		return;
	}
	st->record(err, reason ? reason : "", false);
}

namespace
{
template <class Call>
rd_kafka_resp_err_t slicedCall(rd_kafka_t* rk, const ConnectFailureState& state, int timeoutMs, Call call)
{
	rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR__TIMED_OUT;
	const WaitResult r = waitSliced(timeoutMs, state,
		[&](int32_t slice) {
			const auto t0 = std::chrono::steady_clock::now();
			err = call(slice);
			if (err == RD_KAFKA_RESP_ERR__TIMED_OUT)
				return false;
			if (isRetriableWaitError(err))
			{
				// returned early: wait out the rest of the slice serving callbacks
				rd_kafka_poll(rk, remainingOf(slice, t0));
				return false;
			}
			return true;
		},
		[&] { rd_kafka_poll(rk, 0); });
	return r == WaitResult::Fatal ? RD_KAFKA_RESP_ERR__SSL : err;
}
} // namespace

rd_kafka_resp_err_t slicedMetadata(rd_kafka_t* rk, const ConnectFailureState& state, int allTopics,
                                   rd_kafka_topic_t* topic, const rd_kafka_metadata_t** out, int timeoutMs)
{
	return slicedCall(rk, state, timeoutMs,
		[&](int32_t slice) { return rd_kafka_metadata(rk, allTopics, topic, out, slice); });
}

rd_kafka_resp_err_t slicedWatermarks(rd_kafka_t* rk, const ConnectFailureState& state, const char* topic,
                                     int32_t partition, int64_t* low, int64_t* high, int timeoutMs)
{
	return slicedCall(rk, state, timeoutMs,
		[&](int32_t slice) { return rd_kafka_query_watermark_offsets(rk, topic, partition, low, high, slice); });
}

rd_kafka_resp_err_t slicedCommitted(rd_kafka_t* rk, const ConnectFailureState& state,
                                    rd_kafka_topic_partition_list_t* partitions, int timeoutMs)
{
	return slicedCall(rk, state, timeoutMs,
		[&](int32_t slice) { return rd_kafka_committed(rk, partitions, slice); });
}

rd_kafka_event_t* slicedQueuePoll(rd_kafka_t* rk, rd_kafka_queue_t* queue,
                                  const ConnectFailureState& state, int timeoutMs)
{
	rd_kafka_event_t* ev = nullptr;
	waitSliced(timeoutMs, state,
		[&](int32_t slice) { ev = rd_kafka_queue_poll(queue, slice); return ev != nullptr; },
		[&] { rd_kafka_poll(rk, 0); });
	return ev;
}

} // namespace connect_failure
