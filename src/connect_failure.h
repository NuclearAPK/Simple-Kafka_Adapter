#pragma once

// Unified handling of connection failures (SSL handshake, authentication, authorization).
// librdkafka reports such errors asynchronously on its broker threads and then keeps
// retrying forever, so a blocking call would otherwise wait for its whole timeout and
// return a generic "Broker transport failure". See specs/ssl-fail-fast.md.

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>

#include <librdkafka/rdkafka.h>

namespace connect_failure
{

enum class FailureClass
{
	None,       // not an error
	Transient,  // broker may recover: keep waiting (transport, all brokers down, resolve)
	Fatal       // configuration problem: retrying is pointless (SSL, auth, authorization)
};

// R1: classifies a librdkafka error code (rd_kafka_resp_err_t / RdKafka::ErrorCode value)
FailureClass classifyKafkaError(int err, bool fatalFlag);

// True for network-level errors a blocking call may return before its timeout while the
// broker is unreachable (transport, all brokers down, resolve, timed out). Only these are
// retried inside a sliced wait; any other result is final.
bool isRetriableWaitError(int err);

// Settings that influence the hint text
struct HintContext
{
	std::string brokers;                 // bootstrap.servers, used for the host name
	std::string endpointIdentification;  // ssl.endpoint.identification.algorithm ("" = default https)
	std::string securityProtocol;        // security.protocol
	bool http = false;                   // Schema Registry (curl): TLS is not configured by librdkafka settings
};

// R4: hint for a librdkafka/curl error text; empty when nothing useful can be said
std::string connectHint(const std::string& errorText, const HintContext& ctx);

// Error text as valid UTF-8. OS texts come in the system code page (schannel messages in
// curl's error buffer, socket errors in librdkafka on a Russian Windows are CP1251) and
// invalid UTF-8 in msg_err breaks its conversion for 1C. Valid UTF-8 is returned as is;
// on Windows other text is converted from CP_ACP, elsewhere non-ASCII bytes become '?'.
std::string ensureUtf8(const std::string& text);

// First host name from a broker list ("ssl://a:9093,b:9093" -> "a")
std::string firstBrokerHost(const std::string& brokers);

// R2: thread-safe state of one client; written from librdkafka callbacks
class ConnectFailureState
{
public:
	void record(int err, const std::string& text, bool fatalFlag);
	void recordFatalText(const std::string& text);  // for errors without a librdkafka code (curl)
	bool hasFatal() const;
	std::string fatalText() const;  // first fatal error
	std::string lastText() const;   // last error of any class
	void reset();

private:
	mutable std::mutex mtx;
	bool fatal = false;
	std::string firstFatal;
	std::string last;
};

// R3: "<operation>: <fatal text> | <hint>" when a fatal error is known,
// otherwise "<operation>: <baseError> | <last async error>" (R5); operation may be empty
std::string formatConnectError(std::string_view operation, std::string_view baseError,
                               const ConnectFailureState& state, const HintContext& ctx);

constexpr int32_t kSliceMs = 500;

// Milliseconds left of a slice started at t0 (never negative)
inline int remainingOf(int32_t slice, std::chrono::steady_clock::time_point t0)
{
	const auto spent = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
	return spent >= slice ? 0 : static_cast<int>(slice - spent);
}

enum class WaitResult
{
	Done,     // step reported completion (success or a non-timeout error)
	Fatal,    // a fatal connection error was recorded meanwhile
	Timeout   // timeout elapsed
};

// R6: calls step(sliceMs) until it returns true, the timeout elapses or a fatal error
// appears in state. pump() runs between slices so librdkafka callbacks get served.
// A negative timeout means "wait forever".
template <class Step, class Pump>
WaitResult waitSliced(int32_t timeoutMs, const ConnectFailureState& state, Step step, Pump pump)
{
	using clock = std::chrono::steady_clock;
	const bool infinite = timeoutMs < 0;
	const auto deadline = clock::now() + std::chrono::milliseconds(infinite ? 0 : timeoutMs);

	for (;;)
	{
		if (state.hasFatal())
			return WaitResult::Fatal;

		int32_t slice = kSliceMs;
		if (!infinite)
		{
			const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - clock::now()).count();
			if (left <= 0)
				return WaitResult::Timeout;
			if (left < slice)
				slice = static_cast<int32_t>(left);
		}

		if (step(slice))
			return WaitResult::Done;

		pump();
	}
}

// ---- librdkafka C API helpers ----

// error_cb for rd_kafka_conf_set_error_cb; opaque must point to a ConnectFailureState
// (rd_kafka_conf_set_opaque) that outlives the client.
void rdkafkaErrorCallback(rd_kafka_t* rk, int err, const char* reason, void* opaque);

// Sliced equivalents of blocking C API calls (R6). Network-level results are retried
// until timeout; on a fatal connection error RD_KAFKA_RESP_ERR__SSL is returned early.
rd_kafka_resp_err_t slicedMetadata(rd_kafka_t* rk, const ConnectFailureState& state, int allTopics,
                                   rd_kafka_topic_t* topic, const rd_kafka_metadata_t** out, int timeoutMs);
rd_kafka_resp_err_t slicedWatermarks(rd_kafka_t* rk, const ConnectFailureState& state, const char* topic,
                                     int32_t partition, int64_t* low, int64_t* high, int timeoutMs);
rd_kafka_resp_err_t slicedCommitted(rd_kafka_t* rk, const ConnectFailureState& state,
                                    rd_kafka_topic_partition_list_t* partitions, int timeoutMs);
// Waits for an event on queue; nullptr on timeout or fatal connection error
rd_kafka_event_t* slicedQueuePoll(rd_kafka_t* rk, rd_kafka_queue_t* queue,
                                  const ConnectFailureState& state, int timeoutMs);

} // namespace connect_failure
