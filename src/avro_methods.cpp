// Avro serialization/deserialization methods for SimpleKafka1C

#include <boost/json.hpp>
#include <boost/json/monotonic_resource.hpp>

#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include <avro/Compiler.hh>
#include <avro/Types.hh>
#include <avro/DataFile.hh>
#include <avro/GenericDatum.hh>
#include <avro/Generic.hh>
#include <avro/LogicalType.hh>

#include <fstream>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <cstdint>
#include <cstdlib>
#include <algorithm>
#include <cmath>

#include "SimpleKafka1C.h"

//================================== Avro logical types: helpers ==================

namespace {

// --- Big-integer helpers for Avro decimal (no external bigint dependency).
// Internal representation: sign + decimal digits string.

// Multiply unsigned magnitude (big-endian byte vector) by small factor, add addend.
// Used in: digits-string -> magnitude bytes.
static void bigMulAdd(std::vector<uint8_t>& bytes, uint32_t mul, uint32_t add)
{
	uint64_t carry = add;
	for (auto it = bytes.rbegin(); it != bytes.rend(); ++it)
	{
		uint64_t v = static_cast<uint64_t>(*it) * mul + carry;
		*it = static_cast<uint8_t>(v & 0xFF);
		carry = v >> 8;
	}
	while (carry > 0)
	{
		bytes.insert(bytes.begin(), static_cast<uint8_t>(carry & 0xFF));
		carry >>= 8;
	}
}

// Convert decimal magnitude string ("12345") to minimal big-endian byte magnitude.
static std::vector<uint8_t> digitsToBytes(const std::string& digits)
{
	std::vector<uint8_t> result;
	for (char c : digits)
	{
		if (c < '0' || c > '9') continue;
		bigMulAdd(result, 10, static_cast<uint32_t>(c - '0'));
	}
	if (result.empty()) result.push_back(0);
	// strip leading zeros
	size_t firstNonZero = 0;
	while (firstNonZero + 1 < result.size() && result[firstNonZero] == 0)
		++firstNonZero;
	if (firstNonZero > 0)
		result.erase(result.begin(), result.begin() + firstNonZero);
	return result;
}

// Two's complement encoding of signed integer (decimal digits + sign) into
// minimal big-endian bytes (Avro decimal spec) or fixed-size bytes.
static bool encodeDecimalBytes(bool negative, const std::string& digits,
                                std::vector<uint8_t>& out, size_t fixedSize,
                                std::string& err)
{
	// magnitude bytes (positive)
	std::vector<uint8_t> mag = digitsToBytes(digits);

	if (!negative)
	{
		// ensure top bit is 0 — prepend 0x00 if needed
		if ((mag[0] & 0x80) != 0)
			mag.insert(mag.begin(), 0x00);
		// minimise: drop leading 0x00 if next byte still has top bit clear
		while (mag.size() > 1 && mag[0] == 0x00 && (mag[1] & 0x80) == 0)
			mag.erase(mag.begin());
	}
	else
	{
		// two's complement: invert bits and add 1
		// Compute width in bytes that fits the magnitude with sign bit set
		// First, get the result of (-mag) in two's complement of arbitrary width.
		// Strategy: compute (2^(8*n) - mag) for the smallest n such that result fits with top bit = 1.
		size_t n = mag.size();
		// If the magnitude top bit is already 1, we need one extra byte
		// (e.g. mag = 0x80 -> -128 fits in 1 byte 0x80, but mag = 0x81 needs 2 bytes 0xFF7F).
		// We handle this by computing inversion + 1 and checking sign bit afterwards.
		std::vector<uint8_t> tc(n, 0);
		for (size_t i = 0; i < n; ++i) tc[i] = ~mag[i];
		// add 1
		int carry = 1;
		for (size_t i = n; i > 0 && carry; --i)
		{
			int v = static_cast<int>(tc[i - 1]) + carry;
			tc[i - 1] = static_cast<uint8_t>(v & 0xFF);
			carry = v >> 8;
		}
		// if top bit of tc is 0, magnitude was exactly a power-of-256 boundary (e.g. mag=0x80)
		// — already correct as 0x80 represents -128. Otherwise if top bit is 0, we must prepend 0xFF.
		if ((tc[0] & 0x80) == 0)
			tc.insert(tc.begin(), 0xFF);
		// minimise: drop leading 0xFF if next byte still has top bit set
		while (tc.size() > 1 && tc[0] == 0xFF && (tc[1] & 0x80) != 0)
			tc.erase(tc.begin());
		mag = std::move(tc);
	}

	if (fixedSize > 0)
	{
		// pad to fixedSize on the left with sign byte
		if (mag.size() > fixedSize)
		{
			err = "Decimal value does not fit into fixed of size " + std::to_string(fixedSize);
			return false;
		}
		uint8_t pad = negative ? 0xFF : 0x00;
		while (mag.size() < fixedSize)
			mag.insert(mag.begin(), pad);
	}

	out = std::move(mag);
	return true;
}

// Decode big-endian two's complement bytes into (sign, digits) decimal representation.
static void decodeDecimalBytes(const uint8_t* data, size_t size,
                                bool& negative, std::string& digitsOut)
{
	if (size == 0) { negative = false; digitsOut = "0"; return; }
	negative = (data[0] & 0x80) != 0;

	std::vector<uint8_t> mag(data, data + size);
	if (negative)
	{
		// two's complement: invert + 1
		for (auto& b : mag) b = ~b;
		int carry = 1;
		for (size_t i = mag.size(); i > 0 && carry; --i)
		{
			int v = static_cast<int>(mag[i - 1]) + carry;
			mag[i - 1] = static_cast<uint8_t>(v & 0xFF);
			carry = v >> 8;
		}
	}
	// strip leading zeros
	size_t firstNonZero = 0;
	while (firstNonZero + 1 < mag.size() && mag[firstNonZero] == 0)
		++firstNonZero;
	if (firstNonZero > 0)
		mag.erase(mag.begin(), mag.begin() + firstNonZero);

	// convert magnitude to decimal digits via long division by 10
	if (mag.size() == 1 && mag[0] == 0) { digitsOut = "0"; negative = false; return; }

	std::string reversed;
	while (!(mag.size() == 1 && mag[0] == 0))
	{
		uint32_t rem = 0;
		for (size_t i = 0; i < mag.size(); ++i)
		{
			uint32_t cur = (rem << 8) | mag[i];
			mag[i] = static_cast<uint8_t>(cur / 10);
			rem = cur % 10;
		}
		reversed.push_back(static_cast<char>('0' + rem));
		// trim leading zeros for next iteration
		size_t fz = 0;
		while (fz + 1 < mag.size() && mag[fz] == 0) ++fz;
		if (fz > 0) mag.erase(mag.begin(), mag.begin() + fz);
	}
	std::reverse(reversed.begin(), reversed.end());
	digitsOut = reversed.empty() ? "0" : reversed;
}

// Compose a JSON-number decimal literal from sign, unscaled digits and scale.
// Result is a valid JSON number (no quotes).
static std::string formatDecimalJson(bool negative, const std::string& unscaledDigits, int scale)
{
	std::string digits = unscaledDigits;
	if (digits.empty()) digits = "0";
	if (digits == "0") return "0";

	std::string result;
	if (scale <= 0)
	{
		result = digits;
		if (scale < 0) result.append(static_cast<size_t>(-scale), '0');
	}
	else
	{
		int sc = scale;
		if (static_cast<int>(digits.size()) > sc)
		{
			size_t pointPos = digits.size() - sc;
			result = digits.substr(0, pointPos) + "." + digits.substr(pointPos);
		}
		else
		{
			// 0.<leading zeros><digits>
			std::string frac(sc - digits.size(), '0');
			frac += digits;
			result = "0." + frac;
		}
		// strip trailing zeros after the decimal point? Keep them for full precision.
	}
	if (negative) result.insert(result.begin(), '-');
	return result;
}

// Parse number-like JSON value (number or string) into sign + unscaled digits at given scale.
// Returns false on error.
static bool parseDecimalToUnscaled(const boost::json::value& v, int scale,
                                    bool& negative, std::string& unscaledDigits,
                                    std::string& err)
{
	std::string s;
	if (v.is_string())
	{
		s = std::string(v.as_string());
	}
	else if (v.is_int64())
	{
		int64_t x = v.as_int64();
		if (x < 0) { negative = true; x = -x; } else negative = false;
		s = std::to_string(x);
	}
	else if (v.is_uint64())
	{
		negative = false;
		s = std::to_string(v.as_uint64());
	}
	else if (v.is_double())
	{
		// Format with enough precision to preserve scale.
		double d = v.as_double();
		std::ostringstream oss;
		// Up to 17 significant digits for double, plus extra for scale.
		oss.precision(17);
		oss << std::fixed << d;
		s = oss.str();
	}
	else if (v.is_null())
	{
		err = "Decimal value is null";
		return false;
	}
	else
	{
		err = "Decimal value must be a number or string";
		return false;
	}

	// trim spaces
	while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) s.erase(s.begin());
	while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) s.pop_back();
	if (s.empty()) { err = "Empty decimal string"; return false; }

	negative = false;
	size_t pos = 0;
	if (s[pos] == '+') { ++pos; }
	else if (s[pos] == '-') { negative = true; ++pos; }

	std::string intPart, fracPart;
	bool sawDot = false;
	int expPart = 0;
	for (; pos < s.size(); ++pos)
	{
		char c = s[pos];
		if (c >= '0' && c <= '9')
		{
			if (sawDot) fracPart.push_back(c);
			else intPart.push_back(c);
		}
		else if (c == '.' && !sawDot)
		{
			sawDot = true;
		}
		else if (c == 'e' || c == 'E')
		{
			// scientific notation
			std::string expStr = s.substr(pos + 1);
			if (expStr.empty()) { err = "Invalid decimal: missing exponent"; return false; }
			try { expPart = std::stoi(expStr); }
			catch (...) { err = "Invalid decimal exponent"; return false; }
			break;
		}
		else
		{
			err = std::string("Invalid character in decimal: '") + c + "'";
			return false;
		}
	}

	// Compose unscaled with given scale.
	// effective fractional digits = fracPart.size() - expPart
	int fracLen = static_cast<int>(fracPart.size()) - expPart;
	// shift integer/fraction by expPart
	if (expPart > 0)
	{
		// move expPart digits from fracPart to intPart
		int take = std::min<int>(expPart, static_cast<int>(fracPart.size()));
		intPart += fracPart.substr(0, take);
		fracPart.erase(0, take);
		int remaining = expPart - take;
		if (remaining > 0)
		{
			intPart.append(remaining, '0');
		}
	}
	else if (expPart < 0)
	{
		// move -expPart digits from intPart to front of fracPart
		int shift = -expPart;
		if (static_cast<int>(intPart.size()) >= shift)
		{
			fracPart = intPart.substr(intPart.size() - shift) + fracPart;
			intPart.erase(intPart.size() - shift);
		}
		else
		{
			// prepend zeros
			std::string z(shift - intPart.size(), '0');
			fracPart = z + intPart + fracPart;
			intPart.clear();
		}
	}

	// Now adjust to target scale.
	int curFrac = static_cast<int>(fracPart.size());
	if (curFrac < scale)
	{
		fracPart.append(scale - curFrac, '0');
	}
	else if (curFrac > scale)
	{
		// Round half-up at position 'scale' (banker's would be more accurate, but keep simple)
		// Truncation with rounding on the next digit.
		std::string keep = fracPart.substr(0, scale);
		char nextDigit = fracPart[scale];
		fracPart = keep;
		if (nextDigit >= '5')
		{
			// add 1 to the lowest digit
			std::string all = intPart + fracPart;
			if (all.empty()) all = "0";
			int i = static_cast<int>(all.size()) - 1;
			int carry = 1;
			while (i >= 0 && carry)
			{
				int d = (all[i] - '0') + carry;
				all[i] = static_cast<char>('0' + (d % 10));
				carry = d / 10;
				--i;
			}
			if (carry) all.insert(all.begin(), '1');
			// split back
			int splitPos = static_cast<int>(all.size()) - scale;
			if (splitPos < 0) { intPart.clear(); fracPart = std::string(-splitPos, '0') + all; }
			else { intPart = all.substr(0, splitPos); fracPart = all.substr(splitPos); }
		}
	}
	// Strip leading zeros from intPart (keep at least one)
	size_t firstNonZero = 0;
	while (firstNonZero + 1 < intPart.size() && intPart[firstNonZero] == '0') ++firstNonZero;
	if (firstNonZero > 0) intPart.erase(0, firstNonZero);
	if (intPart.empty()) intPart = "0";

	unscaledDigits = (intPart == "0" ? std::string() : intPart) + fracPart;
	if (unscaledDigits.empty()) unscaledDigits = "0";
	// Strip leading zeros from unscaled
	firstNonZero = 0;
	while (firstNonZero + 1 < unscaledDigits.size() && unscaledDigits[firstNonZero] == '0') ++firstNonZero;
	if (firstNonZero > 0) unscaledDigits.erase(0, firstNonZero);

	if (unscaledDigits == "0") negative = false;

	(void)fracLen;
	return true;
}

// --- ISO-8601 helpers for date / time-millis / timestamp-millis logical types.

static bool isDigits(const std::string& s, size_t from, size_t len)
{
	if (from + len > s.size()) return false;
	for (size_t i = 0; i < len; ++i)
		if (s[from + i] < '0' || s[from + i] > '9') return false;
	return true;
}

// Days from civil date (proleptic Gregorian) — Howard Hinnant's algorithm.
static int64_t daysFromCivil(int y, unsigned m, unsigned d)
{
	y -= m <= 2;
	const int era = (y >= 0 ? y : y - 399) / 400;
	const unsigned yoe = static_cast<unsigned>(y - era * 400);
	const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
	const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
	return era * 146097LL + static_cast<int64_t>(doe) - 719468LL;
}

static void civilFromDays(int64_t z, int& y, unsigned& m, unsigned& d)
{
	z += 719468;
	const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
	const unsigned doe = static_cast<unsigned>(z - era * 146097);
	const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
	const int yi = static_cast<int>(yoe) + static_cast<int>(era) * 400;
	const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
	const unsigned mp = (5 * doy + 2) / 153;
	d = doy - (153 * mp + 2) / 5 + 1;
	m = mp < 10 ? mp + 3 : mp - 9;
	y = yi + (m <= 2 ? 1 : 0);
}

// Parse "YYYY-MM-DD" into days since 1970-01-01. Returns false if format invalid.
static bool parseIsoDate(const std::string& s, int32_t& outDays, std::string& err)
{
	if (s.size() < 10 || s[4] != '-' || s[7] != '-' ||
	    !isDigits(s, 0, 4) || !isDigits(s, 5, 2) || !isDigits(s, 8, 2))
	{
		err = "Invalid ISO date format, expected YYYY-MM-DD";
		return false;
	}
	int y = std::atoi(s.substr(0, 4).c_str());
	int mo = std::atoi(s.substr(5, 2).c_str());
	int d = std::atoi(s.substr(8, 2).c_str());
	if (mo < 1 || mo > 12 || d < 1 || d > 31)
	{
		err = "ISO date out of range";
		return false;
	}
	int64_t days = daysFromCivil(y, static_cast<unsigned>(mo), static_cast<unsigned>(d));
	outDays = static_cast<int32_t>(days);
	return true;
}

static std::string formatIsoDate(int32_t days)
{
	int y; unsigned m, d;
	civilFromDays(days, y, m, d);
	char buf[16];
	std::snprintf(buf, sizeof(buf), "%04d-%02u-%02u", y, m, d);
	return std::string(buf);
}

// Parse "HH:MM:SS[.fff]" into milliseconds since midnight.
// Also accepts a full ISO datetime "YYYY-MM-DDTHH:MM:SS[.fff]" (or with a space
// separator) and uses only its time part — 1C serializes the Date type with a
// date prefix even when only the time is meaningful (e.g. "0001-01-01T09:00:00").
static bool parseIsoTimeMillis(const std::string& s, int32_t& outMs, std::string& err)
{
	std::string t = s;
	if (s.size() >= 11 && (s[10] == 'T' || s[10] == ' ') &&
	    s[4] == '-' && s[7] == '-')
		t = s.substr(11);

	if (t.size() < 8 || t[2] != ':' || t[5] != ':' ||
	    !isDigits(t, 0, 2) || !isDigits(t, 3, 2) || !isDigits(t, 6, 2))
	{
		err = "Invalid ISO time format, expected HH:MM:SS[.fff]";
		return false;
	}
	int h = std::atoi(t.substr(0, 2).c_str());
	int mi = std::atoi(t.substr(3, 2).c_str());
	int se = std::atoi(t.substr(6, 2).c_str());
	int ms = 0;
	if (t.size() > 8 && t[8] == '.')
	{
		std::string frac;
		for (size_t i = 9; i < t.size() && t[i] >= '0' && t[i] <= '9'; ++i)
			frac.push_back(t[i]);
		if (frac.size() > 3) frac = frac.substr(0, 3);
		while (frac.size() < 3) frac.push_back('0');
		ms = std::atoi(frac.c_str());
	}
	if (h < 0 || h > 23 || mi < 0 || mi > 59 || se < 0 || se > 59)
	{
		err = "ISO time out of range";
		return false;
	}
	outMs = ((h * 60 + mi) * 60 + se) * 1000 + ms;
	return true;
}

static std::string formatIsoTimeMillis(int32_t ms)
{
	if (ms < 0) ms = 0;
	int h = ms / 3600000;
	ms -= h * 3600000;
	int mi = ms / 60000;
	ms -= mi * 60000;
	int se = ms / 1000;
	int frac = ms - se * 1000;
	char buf[32];
	// Emit fractional part only when milliseconds are actually present.
	if (frac != 0)
		std::snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%03d", h, mi, se, frac);
	else
		std::snprintf(buf, sizeof(buf), "%02d:%02d:%02d", h, mi, se);
	return std::string(buf);
}

// Parse "YYYY-MM-DDTHH:MM:SS[.fff][Z|+hh:mm|-hh:mm]" into ms since UTC epoch.
static bool parseIsoTimestampMillis(const std::string& s, int64_t& outMs, std::string& err)
{
	if (s.size() < 19 || (s[10] != 'T' && s[10] != ' '))
	{
		err = "Invalid ISO timestamp, expected YYYY-MM-DDTHH:MM:SS[.fff][Z]";
		return false;
	}
	int32_t days = 0;
	if (!parseIsoDate(s.substr(0, 10), days, err)) return false;
	int32_t timeMs = 0;
	if (!parseIsoTimeMillis(s.substr(11, 8) + (s.size() > 19 && s[19] == '.' ? s.substr(19) : std::string()), timeMs, err))
		return false;
	// timezone offset
	int tzMin = 0;
	size_t tzPos = s.find_first_of("Zz+", 19);
	if (tzPos == std::string::npos)
	{
		// Maybe '-' offset; search for sign past possible fractional digits
		for (size_t i = 19; i < s.size(); ++i)
		{
			if (s[i] == '-' || s[i] == '+' || s[i] == 'Z' || s[i] == 'z') { tzPos = i; break; }
		}
	}
	if (tzPos != std::string::npos)
	{
		char c = s[tzPos];
		if (c == 'Z' || c == 'z')
		{
			tzMin = 0;
		}
		else
		{
			int sign = (c == '+') ? 1 : -1;
			std::string off = s.substr(tzPos + 1);
			int oh = 0, om = 0;
			if (off.size() >= 5 && off[2] == ':')
			{
				oh = std::atoi(off.substr(0, 2).c_str());
				om = std::atoi(off.substr(3, 2).c_str());
			}
			else if (off.size() >= 4)
			{
				oh = std::atoi(off.substr(0, 2).c_str());
				om = std::atoi(off.substr(2, 2).c_str());
			}
			else if (off.size() >= 2)
			{
				oh = std::atoi(off.substr(0, 2).c_str());
			}
			tzMin = sign * (oh * 60 + om);
		}
	}
	int64_t epochMs = static_cast<int64_t>(days) * 86400000LL + timeMs;
	epochMs -= static_cast<int64_t>(tzMin) * 60000LL;
	outMs = epochMs;
	return true;
}

static std::string formatIsoTimestampMillis(int64_t ms)
{
	int64_t days = ms / 86400000LL;
	int64_t rem = ms - days * 86400000LL;
	if (rem < 0) { rem += 86400000LL; --days; }
	int y; unsigned m, d;
	civilFromDays(days, y, m, d);
	int h = static_cast<int>(rem / 3600000);
	rem -= static_cast<int64_t>(h) * 3600000;
	int mi = static_cast<int>(rem / 60000);
	rem -= static_cast<int64_t>(mi) * 60000;
	int se = static_cast<int>(rem / 1000);
	int frac = static_cast<int>(rem - static_cast<int64_t>(se) * 1000);
	char buf[40];
	// Avro timestamp-millis has no timezone (always UTC) — omit the trailing 'Z',
	// and emit the fractional part only when milliseconds are actually present.
	if (frac != 0)
		std::snprintf(buf, sizeof(buf), "%04d-%02u-%02uT%02d:%02d:%02d.%03d",
		              y, m, d, h, mi, se, frac);
	else
		std::snprintf(buf, sizeof(buf), "%04d-%02u-%02uT%02d:%02d:%02d",
		              y, m, d, h, mi, se);
	return std::string(buf);
}

} // anonymous namespace

//================================== Avro ==========================================

bool SimpleKafka1C::putAvroSchema(const variant_t& schemaJsonName, const variant_t& schemaJson)
{
	try
	{
		// Проверяем, существует ли схема с таким именем
		auto it = schemesMap.find(std::get<std::string>(schemaJsonName));

		if (it == schemesMap.end())
		{
			// Схема не существует, компилируем и добавляем ее в map
			const avro::ValidSchema compiledScheme = avro::compileJsonSchemaFromString(std::get<std::string>(schemaJson));
			schemesMap[std::get<std::string>(schemaJsonName)] = compiledScheme;
		}
	}
	catch (std::exception const& ex)
	{
		msg_err = std::string("Scheme compile error: ") + ex.what();
	}

	return msg_err.empty();
}

// Вспомогательная функция для рекурсивного заполнения GenericDatum из JSON
static bool fillAvroFromJson(avro::GenericDatum& datum, const boost::json::value& jsonValue, std::string& errMsg, const std::string& path = "")
{
	// Обработка union типов
	if (datum.isUnion())
	{
		if (jsonValue.is_null())
		{
			// null - первая ветка union (обычно ["null", "type"])
			datum.selectBranch(0);
			return true;
		}
		// Не null - выбираем вторую ветку
		datum.selectBranch(1);
	}

	avro::LogicalType lt = datum.logicalType();

	switch (datum.type())
	{
	case avro::AVRO_STRING:
		if (!jsonValue.is_string())
		{
			errMsg = "Expected string at " + path;
			return false;
		}
		datum.value<std::string>() = std::string(jsonValue.as_string());
		return true;

	case avro::AVRO_LONG:
	{
		if (lt.type() == avro::LogicalType::TIMESTAMP_MILLIS)
		{
			int64_t ms = 0;
			if (jsonValue.is_string())
			{
				std::string sub;
				if (!parseIsoTimestampMillis(std::string(jsonValue.as_string()), ms, sub))
				{
					errMsg = "Invalid timestamp-millis at " + path + ": " + sub;
					return false;
				}
			}
			else if (jsonValue.is_int64()) ms = jsonValue.as_int64();
			else if (jsonValue.is_uint64()) ms = static_cast<int64_t>(jsonValue.as_uint64());
			else if (jsonValue.is_double()) ms = static_cast<int64_t>(jsonValue.as_double());
			else { errMsg = "Expected timestamp-millis (string or number) at " + path; return false; }
			datum.value<int64_t>() = ms;
			return true;
		}
		if (!jsonValue.is_int64())
		{
			errMsg = "Expected long at " + path;
			return false;
		}
		datum.value<int64_t>() = jsonValue.as_int64();
		return true;
	}

	case avro::AVRO_INT:
	{
		if (lt.type() == avro::LogicalType::DATE)
		{
			int32_t days = 0;
			if (jsonValue.is_string())
			{
				std::string sub;
				if (!parseIsoDate(std::string(jsonValue.as_string()), days, sub))
				{
					errMsg = "Invalid date at " + path + ": " + sub;
					return false;
				}
			}
			else if (jsonValue.is_int64()) days = static_cast<int32_t>(jsonValue.as_int64());
			else { errMsg = "Expected date (string or number) at " + path; return false; }
			datum.value<int32_t>() = days;
			return true;
		}
		if (lt.type() == avro::LogicalType::TIME_MILLIS)
		{
			int32_t ms = 0;
			if (jsonValue.is_string())
			{
				std::string sub;
				if (!parseIsoTimeMillis(std::string(jsonValue.as_string()), ms, sub))
				{
					errMsg = "Invalid time-millis at " + path + ": " + sub;
					return false;
				}
			}
			else if (jsonValue.is_int64()) ms = static_cast<int32_t>(jsonValue.as_int64());
			else { errMsg = "Expected time-millis (string or number) at " + path; return false; }
			datum.value<int32_t>() = ms;
			return true;
		}
		if (!jsonValue.is_int64())
		{
			errMsg = "Expected int at " + path;
			return false;
		}
		datum.value<int32_t>() = static_cast<int32_t>(jsonValue.as_int64());
		return true;
	}

	case avro::AVRO_FLOAT:
		if (jsonValue.is_double())
			datum.value<float>() = static_cast<float>(jsonValue.as_double());
		else if (jsonValue.is_int64())
			datum.value<float>() = static_cast<float>(jsonValue.as_int64());
		else
		{
			errMsg = "Expected float at " + path;
			return false;
		}
		return true;

	case avro::AVRO_DOUBLE:
		if (jsonValue.is_double())
			datum.value<double>() = jsonValue.as_double();
		else if (jsonValue.is_int64())
			datum.value<double>() = static_cast<double>(jsonValue.as_int64());
		else
		{
			errMsg = "Expected double at " + path;
			return false;
		}
		return true;

	case avro::AVRO_BOOL:
		if (!jsonValue.is_bool())
		{
			errMsg = "Expected bool at " + path;
			return false;
		}
		datum.value<bool>() = jsonValue.as_bool();
		return true;

	case avro::AVRO_NULL:
		datum.value<avro::null>() = avro::null();
		return true;

	case avro::AVRO_BYTES:
	{
		if (lt.type() == avro::LogicalType::DECIMAL)
		{
			bool neg = false;
			std::string unscaled;
			std::string sub;
			if (!parseDecimalToUnscaled(jsonValue, lt.scale(), neg, unscaled, sub))
			{
				errMsg = "Invalid decimal at " + path + ": " + sub;
				return false;
			}
			std::vector<uint8_t> bytes;
			if (!encodeDecimalBytes(neg, unscaled, bytes, 0, sub))
			{
				errMsg = "Decimal encoding failed at " + path + ": " + sub;
				return false;
			}
			datum.value<std::vector<uint8_t>>() = std::move(bytes);
			return true;
		}
		if (!jsonValue.is_string())
		{
			errMsg = "Expected string (bytes) at " + path;
			return false;
		}
		std::string strVal = std::string(jsonValue.as_string());
		std::vector<uint8_t> bytes(strVal.begin(), strVal.end());
		datum.value<std::vector<uint8_t>>() = bytes;
		return true;
	}

	case avro::AVRO_FIXED:
	{
		avro::GenericFixed& fixed = datum.value<avro::GenericFixed>();
		if (lt.type() == avro::LogicalType::DECIMAL)
		{
			bool neg = false;
			std::string unscaled;
			std::string sub;
			if (!parseDecimalToUnscaled(jsonValue, lt.scale(), neg, unscaled, sub))
			{
				errMsg = "Invalid decimal at " + path + ": " + sub;
				return false;
			}
			size_t fixedSize = fixed.value().size();
			if (fixedSize == 0)
			{
				// schema size — fall back to schema node
				fixedSize = fixed.schema()->fixedSize();
			}
			std::vector<uint8_t> bytes;
			if (!encodeDecimalBytes(neg, unscaled, bytes, fixedSize, sub))
			{
				errMsg = "Decimal encoding failed at " + path + ": " + sub;
				return false;
			}
			fixed.value() = std::move(bytes);
			return true;
		}
		if (!jsonValue.is_string())
		{
			errMsg = "Expected string (fixed) at " + path;
			return false;
		}
		std::string strVal = std::string(jsonValue.as_string());
		// Проверка на UUID формат
		if (strVal.length() == 36 && strVal[8] == '-' && strVal[13] == '-')
		{
			std::string hex;
			for (char c : strVal)
			{
				if (c != '-') hex += c;
			}
			std::vector<uint8_t>& bytes = fixed.value();
			bytes.resize(16);
			for (size_t j = 0; j < 16; j++)
			{
				std::string byteStr = hex.substr(j * 2, 2);
				bytes[j] = static_cast<uint8_t>(std::stoul(byteStr, nullptr, 16));
			}
		}
		else
		{
			std::vector<uint8_t>& bytes = fixed.value();
			bytes.assign(strVal.begin(), strVal.end());
		}
		return true;
	}

	case avro::AVRO_RECORD:
	{
		if (!jsonValue.is_object())
		{
			errMsg = "Expected object at " + path;
			return false;
		}
		const boost::json::object& obj = jsonValue.as_object();
		avro::GenericRecord& record = datum.value<avro::GenericRecord>();
		size_t fieldCount = record.fieldCount();
		const avro::NodePtr& schema = record.schema();

		for (size_t i = 0; i < fieldCount; ++i)
		{
			std::string fieldName = schema->nameAt(i);
			auto it = obj.find(fieldName);
			if (it == obj.end())
			{
				// Поле отсутствует - проверяем, есть ли default или это union с null
				avro::GenericDatum& fieldDatum = record.fieldAt(i);
				if (fieldDatum.isUnion())
				{
					// Union с null - устанавливаем null
					fieldDatum.selectBranch(0);
				}
				// Иначе оставляем значение по умолчанию
				continue;
			}
			avro::GenericDatum& fieldDatum = record.fieldAt(i);
			std::string fieldPath = path.empty() ? fieldName : path + "." + fieldName;
			if (!fillAvroFromJson(fieldDatum, it->value(), errMsg, fieldPath))
			{
				return false;
			}
		}
		return true;
	}

	case avro::AVRO_ENUM:
	{
		if (!jsonValue.is_string())
		{
			errMsg = "Expected string (enum symbol) at " + path;
			return false;
		}
		avro::GenericEnum& enumVal = datum.value<avro::GenericEnum>();
		std::string symbol = std::string(jsonValue.as_string());
		enumVal.set(symbol);
		return true;
	}

	case avro::AVRO_ARRAY:
	{
		if (!jsonValue.is_array())
		{
			errMsg = "Expected array at " + path;
			return false;
		}
		const boost::json::array& arr = jsonValue.as_array();
		avro::GenericArray& avroArray = datum.value<avro::GenericArray>();
		const avro::NodePtr& schema = avroArray.schema();
		avroArray.value().clear();

		for (size_t i = 0; i < arr.size(); ++i)
		{
			avro::GenericDatum elemDatum(schema->leafAt(0));
			std::string elemPath = path + "[" + std::to_string(i) + "]";
			if (!fillAvroFromJson(elemDatum, arr[i], errMsg, elemPath))
			{
				return false;
			}
			avroArray.value().push_back(elemDatum);
		}
		return true;
	}

	case avro::AVRO_MAP:
	{
		if (!jsonValue.is_object())
		{
			errMsg = "Expected object (map) at " + path;
			return false;
		}
		const boost::json::object& obj = jsonValue.as_object();
		avro::GenericMap& avroMap = datum.value<avro::GenericMap>();
		const avro::NodePtr& schema = avroMap.schema();
		avroMap.value().clear();

		for (const auto& kv : obj)
		{
			avro::GenericDatum valueDatum(schema->leafAt(1));
			std::string elemPath = path + "[\"" + std::string(kv.key()) + "\"]";
			if (!fillAvroFromJson(valueDatum, kv.value(), errMsg, elemPath))
			{
				return false;
			}
			avroMap.value().push_back(std::make_pair(std::string(kv.key()), valueDatum));
		}
		return true;
	}

	default:
		errMsg = "Unsupported Avro type at " + path;
		return false;
	}
}

bool SimpleKafka1C::convertToAvroFormat(const variant_t& msgJson, const variant_t& schemaJsonName, const variant_t& format, const variant_t& schemaId)
{
	avroFile.clear();
	auto it = schemesMap.find(std::get<std::string>(schemaJsonName));
	avro::ValidSchema schema;

	if (it != schemesMap.end())
	{
		schema = it->second;
	}
	else
	{
		msg_err = "Unknown schema name: " + std::get<std::string>(schemaJsonName);
		return false;
	}

	avro::GenericDatum datum(schema);
	if (datum.type() != avro::AVRO_RECORD)
	{
		msg_err = "Invalid schema: root type must be record";
		return false;
	}

	// Определяем формат вывода: "" / "ocf" / "raw" / "confluent"
	std::string fmtStr;
	if (std::holds_alternative<std::string>(format))
	{
		fmtStr = std::get<std::string>(format);
	}

	int32_t sid = 0;
	if (std::holds_alternative<int32_t>(schemaId))
	{
		sid = std::get<int32_t>(schemaId);
	}

	if (!fmtStr.empty() && fmtStr != "ocf" && fmtStr != "raw" && fmtStr != "confluent")
	{
		msg_err = "Unknown format: " + fmtStr + ". Valid values: ocf, raw, confluent";
		return false;
	}

	bool useOcf = fmtStr.empty() || fmtStr == "ocf";

	// Разбираем исходный JSON
	boost::json::monotonic_resource mr;
	boost::json::value jsonInput;
	try
	{
		jsonInput = boost::json::parse(std::get<std::string>(msgJson), &mr);
	}
	catch (std::exception const& ex)
	{
		msg_err = "Error parsing JSON - ";
		msg_err += ex.what();
		return false;
	}

	try
	{
		// Собираем все записи из JSON
		std::vector<avro::GenericDatum> records;

		// Определяем формат входных данных:
		// 1. Объект {...} - одна запись (новый стандартный формат)
		// 2. Массив [{...}, {...}] - несколько записей (новый стандартный формат)
		// 3. "Столбцовый" формат {"field1": [v1, v2], "field2": [v1, v2]} - для совместимости

		if (jsonInput.is_object())
		{
			const boost::json::object& obj = jsonInput.as_object();

			// Проверяем формат: если первое значение - массив, это "столбцовый" формат
			bool isColumnarFormat = false;
			if (!obj.empty())
			{
				const auto& firstValue = obj.cbegin()->value();
				if (firstValue.is_array())
				{
					// Дополнительная проверка: в стандартном формате array тоже может быть значением поля
					// Проверяем, совпадают ли размеры всех массивов (признак столбцового формата)
					size_t firstSize = firstValue.as_array().size();
					bool allSameSize = true;
					for (const auto& kv : obj)
					{
						if (!kv.value().is_array() || kv.value().as_array().size() != firstSize)
						{
							allSameSize = false;
							break;
						}
					}
					// Если все поля - массивы одинаковой длины, считаем это столбцовым форматом
					// (кроме случая когда длина = 1, тогда это может быть и стандартный формат)
					isColumnarFormat = allSameSize && firstSize > 1;
				}
			}

			if (isColumnarFormat)
			{
				// Старый "столбцовый" формат для совместимости
				const auto first_array = obj.cbegin();
				const size_t numElements = first_array->value().as_array().size();

				for (size_t i = 0; i < numElements; i++)
				{
					boost::json::object jsonRecord;
					for (const auto& kv : obj)
					{
						jsonRecord[kv.key()] = kv.value().as_array().at(i);
					}

					avro::GenericDatum recordDatum(schema);
					if (!fillAvroFromJson(recordDatum, boost::json::value(jsonRecord), msg_err))
					{
						return false;
					}
					records.push_back(std::move(recordDatum));
				}
			}
			else
			{
				// Стандартный формат: одна запись как объект
				if (!fillAvroFromJson(datum, jsonInput, msg_err))
				{
					return false;
				}
				records.push_back(std::move(datum));
			}
		}
		else if (jsonInput.is_array())
		{
			// Стандартный формат: массив записей
			const boost::json::array& arr = jsonInput.as_array();
			for (size_t i = 0; i < arr.size(); ++i)
			{
				avro::GenericDatum recordDatum(schema);
				std::string recordPath = "[" + std::to_string(i) + "]";
				if (!fillAvroFromJson(recordDatum, arr[i], msg_err, recordPath))
				{
					return false;
				}
				records.push_back(std::move(recordDatum));
			}
		}
		else
		{
			msg_err = "JSON must be an object or an array of objects";
			return false;
		}

		if (useOcf)
		{
			// Avro Object Container Format (OCF) через DataFileWriter
			MemoryOutputStream* memOutStr = new MemoryOutputStream(100000);
			std::unique_ptr<avro::OutputStream> os(memOutStr);
			avro::DataFileWriter<avro::GenericDatum> writer(std::move(os), schema);

			for (const auto& record : records)
			{
				writer.write(record);
			}

			writer.flush();
			memOutStr->snapshot(avroFile);
			writer.close();
		}
		else
		{
			// Raw Avro binary encoding
			std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
			avro::EncoderPtr encoder = avro::binaryEncoder();
			encoder->init(*out);

			for (const auto& record : records)
			{
				avro::GenericWriter::write(*encoder, record, schema);
			}
			encoder->flush();

			// Для Confluent Wire Format: magic byte (0x00) + 4 байта schema ID (big-endian)
			if (fmtStr == "confluent")
			{
				avroFile.resize(5);
				avroFile[0] = 0x00;
				avroFile[1] = static_cast<uint8_t>((sid >> 24) & 0xFF);
				avroFile[2] = static_cast<uint8_t>((sid >> 16) & 0xFF);
				avroFile[3] = static_cast<uint8_t>((sid >> 8) & 0xFF);
				avroFile[4] = static_cast<uint8_t>(sid & 0xFF);
			}

			// Копируем raw Avro данные
			std::unique_ptr<avro::InputStream> inStream = avro::memoryInputStream(*out);
			const uint8_t* data;
			size_t len;
			while (inStream->next(&data, &len))
			{
				avroFile.insert(avroFile.end(), data, data + len);
			}
		}
	}
	catch (std::exception const& ex)
	{
		// Присваиваем (а не +=), чтобы не смешать с возможной предыдущей ошибкой
		msg_err = std::string("Error converting to Avro: ") + ex.what();
	}

	return msg_err.empty();
}

bool SimpleKafka1C::saveAvroFile(const variant_t& fileName)
{
	if (avroFile.empty())
	{
		msg_err = "AVRO data is empty";
		return false;
	}

	try
	{
		std::ofstream out(std::get<std::string>(fileName), std::ios::out | std::ios::binary);
		out.write(reinterpret_cast<const char*>(avroFile.data()), avroFile.size());
		out.close();
	}
	catch (std::exception const& ex)
	{
		msg_err = ex.what();
	}

	return msg_err.empty();
}

// Escape JSON string
static std::string escapeJsonString(const std::string& input)
{
	std::ostringstream oss;
	for (char c : input)
	{
		switch (c)
		{
		case '"':  oss << "\\\""; break;
		case '\\': oss << "\\\\"; break;
		case '\b': oss << "\\b"; break;
		case '\f': oss << "\\f"; break;
		case '\n': oss << "\\n"; break;
		case '\r': oss << "\\r"; break;
		case '\t': oss << "\\t"; break;
		default:
			if (static_cast<unsigned char>(c) < 0x20)
			{
				oss << "\\u" << std::hex << std::setfill('0') << std::setw(4) << static_cast<int>(c);
			}
			else
			{
				oss << c;
			}
			break;
		}
	}
	return oss.str();
}

// Convert GenericDatum to JSON string
static std::string convertAvroDatumToJsonString(const avro::GenericDatum& datum)
{
	std::ostringstream oss;
	avro::LogicalType lt = datum.logicalType();

	switch (datum.type())
	{
	case avro::AVRO_NULL:
		oss << "null";
		break;

	case avro::AVRO_BOOL:
		oss << (datum.value<bool>() ? "true" : "false");
		break;

	case avro::AVRO_INT:
	{
		int32_t v = datum.value<int32_t>();
		if (lt.type() == avro::LogicalType::DATE)
			oss << "\"" << formatIsoDate(v) << "\"";
		else if (lt.type() == avro::LogicalType::TIME_MILLIS)
			oss << "\"" << formatIsoTimeMillis(v) << "\"";
		else
			oss << v;
		break;
	}

	case avro::AVRO_LONG:
	{
		int64_t v = datum.value<int64_t>();
		if (lt.type() == avro::LogicalType::TIMESTAMP_MILLIS)
			oss << "\"" << formatIsoTimestampMillis(v) << "\"";
		else
			oss << v;
		break;
	}

	case avro::AVRO_FLOAT:
		oss << std::setprecision(9) << datum.value<float>();
		break;

	case avro::AVRO_DOUBLE:
		oss << std::setprecision(17) << datum.value<double>();
		break;

	case avro::AVRO_STRING:
		oss << "\"" << escapeJsonString(datum.value<std::string>()) << "\"";
		break;

	case avro::AVRO_BYTES:
	{
		const auto& bytes = datum.value<std::vector<uint8_t>>();
		if (lt.type() == avro::LogicalType::DECIMAL)
		{
			bool neg = false;
			std::string digits;
			decodeDecimalBytes(bytes.data(), bytes.size(), neg, digits);
			oss << formatDecimalJson(neg, digits, lt.scale());
			break;
		}
		oss << "\"";
		for (uint8_t byte : bytes)
		{
			oss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(byte);
		}
		oss << "\"";
		break;
	}

	case avro::AVRO_FIXED:
	{
		const auto& fixed = datum.value<avro::GenericFixed>();
		const auto& bytes = fixed.value();
		if (lt.type() == avro::LogicalType::DECIMAL)
		{
			bool neg = false;
			std::string digits;
			decodeDecimalBytes(bytes.data(), bytes.size(), neg, digits);
			oss << formatDecimalJson(neg, digits, lt.scale());
			break;
		}
		oss << "\"";
		for (uint8_t byte : bytes)
		{
			oss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(byte);
		}
		oss << "\"";
		break;
	}

	case avro::AVRO_RECORD:
	{
		const auto& record = datum.value<avro::GenericRecord>();
		oss << "{";
		bool first = true;
		for (size_t i = 0; i < record.fieldCount(); ++i)
		{
			if (!first) oss << ",";
			first = false;
			oss << "\"" << escapeJsonString(record.schema()->nameAt(i)) << "\":";
			oss << convertAvroDatumToJsonString(record.fieldAt(i));
		}
		oss << "}";
		break;
	}

	case avro::AVRO_ENUM:
	{
		const auto& enumVal = datum.value<avro::GenericEnum>();
		oss << "\"" << escapeJsonString(enumVal.symbol()) << "\"";
		break;
	}

	case avro::AVRO_ARRAY:
	{
		const auto& array = datum.value<avro::GenericArray>();
		oss << "[";
		bool first = true;
		for (const auto& item : array.value())
		{
			if (!first) oss << ",";
			first = false;
			oss << convertAvroDatumToJsonString(item);
		}
		oss << "]";
		break;
	}

	case avro::AVRO_MAP:
	{
		const auto& map = datum.value<avro::GenericMap>();
		oss << "{";
		bool first = true;
		for (const auto& kv : map.value())
		{
			if (!first) oss << ",";
			first = false;
			oss << "\"" << escapeJsonString(kv.first) << "\":";
			oss << convertAvroDatumToJsonString(kv.second);
		}
		oss << "}";
		break;
	}

	case avro::AVRO_UNION:
	{
		const auto& unionDatum = datum.value<avro::GenericUnion>().datum();
		oss << convertAvroDatumToJsonString(unionDatum);
		break;
	}

	default:
		oss << "null";
		break;
	}

	return oss.str();
}

variant_t SimpleKafka1C::decodeAvroMessage(const variant_t& avroData, const variant_t& schemaJsonName, const variant_t& asJson)
{
	try
	{
		// Получаем бинарные данные
		const std::vector<char>* dataPtr = nullptr;
		size_t dataSize = 0;

		if (std::holds_alternative<std::vector<char>>(avroData))
		{
			dataPtr = &std::get<std::vector<char>>(avroData);
			dataSize = dataPtr->size();
		}
		else if (std::holds_alternative<std::string>(avroData))
		{
			const std::string& str = std::get<std::string>(avroData);
			messageData.assign(str.begin(), str.end());
			dataPtr = &messageData;
			dataSize = messageData.size();
		}
		else
		{
			msg_err = "Invalid data type for avroData. Expected string or binary data";
			return std::string("");
		}

		if (dataSize == 0)
		{
			msg_err = "AVRO data is empty";
			return std::string("");
		}

		// Безопасное получение параметров
		bool returnAsJson = true;
		if (std::holds_alternative<bool>(asJson))
		{
			returnAsJson = std::get<bool>(asJson);
		}

		std::string schemaName;
		if (std::holds_alternative<std::string>(schemaJsonName))
		{
			schemaName = std::get<std::string>(schemaJsonName);
		}

		// Если схема передана явно, поддерживаем Raw Avro и Confluent Wire Format
		if (!schemaName.empty())
		{
			auto it = schemesMap.find(schemaName);
			if (it == schemesMap.end())
			{
				msg_err = "Schema not found: " + schemaName;
				return std::string("");
			}

			size_t payloadOffset = 0;
			if (static_cast<uint8_t>(dataPtr->at(0)) == 0x00)
			{
				if (dataSize < 5)
				{
					msg_err = "Invalid Confluent Wire Format payload: header is shorter than 5 bytes";
					return std::string("");
				}
				payloadOffset = 5;
			}

			if (dataSize <= payloadOffset)
			{
				msg_err = "AVRO payload is empty after header processing";
				return std::string("");
			}

			avro::ValidSchema schema = it->second;
			std::unique_ptr<avro::InputStream> in = avro::memoryInputStream(
				reinterpret_cast<const uint8_t*>(dataPtr->data() + payloadOffset),
				dataSize - payloadOffset);

			avro::DecoderPtr decoder = avro::binaryDecoder();
			decoder->init(*in);

			avro::GenericReader reader(schema, decoder);
			avro::GenericDatum datum(schema);
			reader.read(datum);

			if (returnAsJson)
			{
				return convertAvroDatumToJsonString(datum);
			}

			std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
			avro::EncoderPtr encoder = avro::binaryEncoder();
			encoder->init(*out);
			avro::GenericWriter::write(*encoder, datum, schema);
			encoder->flush();

			std::unique_ptr<avro::InputStream> inStream = avro::memoryInputStream(*out);
			const uint8_t* data;
			size_t len;
			std::vector<char> result;
			while (inStream->next(&data, &len))
			{
				result.insert(result.end(), data, data + len);
			}
			return result;
		}

		// Создаём поток для чтения из памяти
		std::unique_ptr<avro::InputStream> in = avro::memoryInputStream(
			reinterpret_cast<const uint8_t*>(dataPtr->data()), dataSize);

		// Создаём DataFileReader
		avro::DataFileReader<avro::GenericDatum> reader(std::move(in));

		// Получаем схему из файла
		avro::ValidSchema schema = reader.dataSchema();

		// Используем схему из карты, если указана
		if (!schemaName.empty())
		{
			auto it = schemesMap.find(schemaName);
			if (it != schemesMap.end())
			{
				schema = it->second;
			}
		}

		// Читаем все записи
		std::vector<avro::GenericDatum> records;
		avro::GenericDatum datum(schema);
		while (reader.read(datum))
		{
			records.push_back(datum);
			datum = avro::GenericDatum(schema);
		}

		if (records.empty())
		{
			if (returnAsJson)
			{
				return std::string("{}");
			}
			else
			{
				return std::vector<char>();
			}
		}

		if (returnAsJson)
		{
			// Конвертируем в JSON
			if (records.size() == 1)
			{
				return convertAvroDatumToJsonString(records[0]);
			}
			else
			{
				std::ostringstream oss;
				oss << "[";
				bool first = true;
				for (const auto& record : records)
				{
					if (!first) oss << ",";
					first = false;
					oss << convertAvroDatumToJsonString(record);
				}
				oss << "]";
				return oss.str();
			}
		}
		else
		{
			// Возвращаем raw Avro данные (без OCF контейнера)
			std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
			avro::EncoderPtr encoder = avro::binaryEncoder();
			encoder->init(*out);

			for (const auto& record : records)
			{
				avro::GenericWriter::write(*encoder, record, schema);
			}
			encoder->flush();

			// Копируем данные из output stream
			std::unique_ptr<avro::InputStream> inStream = avro::memoryInputStream(*out);
			const uint8_t* data;
			size_t len;
			std::vector<char> result;
			while (inStream->next(&data, &len))
			{
				result.insert(result.end(), data, data + len);
			}
			return result;
		}
	}
	catch (const avro::Exception& ex)
	{
		msg_err = "AVRO error: ";
		msg_err += ex.what();
		return std::string("");
	}
	catch (const std::exception& ex)
	{
		msg_err = "Error decoding AVRO: ";
		msg_err += ex.what();
		return std::string("");
	}
	catch (...)
	{
		msg_err = "Unknown error decoding AVRO";
		return std::string("");
	}
}

variant_t SimpleKafka1C::getAvroSchema(const variant_t& avroData)
{
	try
	{
		// Получаем бинарные данные
		const std::vector<char>* dataPtr = nullptr;
		size_t dataSize = 0;

		if (std::holds_alternative<std::vector<char>>(avroData))
		{
			dataPtr = &std::get<std::vector<char>>(avroData);
			dataSize = dataPtr->size();
		}
		else if (std::holds_alternative<std::string>(avroData))
		{
			const std::string& str = std::get<std::string>(avroData);
			messageData.assign(str.begin(), str.end());
			dataPtr = &messageData;
			dataSize = messageData.size();
		}
		else
		{
			msg_err = "Invalid data type for avroData. Expected string or binary data";
			return std::string("");
		}

		if (dataSize == 0)
		{
			msg_err = "AVRO data is empty";
			return std::string("");
		}

		// Создаём поток для чтения
		std::unique_ptr<avro::InputStream> in = avro::memoryInputStream(
			reinterpret_cast<const uint8_t*>(dataPtr->data()), dataSize);

		// Создаём DataFileReader
		avro::DataFileReader<avro::GenericDatum> reader(std::move(in));

		// Получаем схему и возвращаем как JSON строку
		avro::ValidSchema schema = reader.dataSchema();
		std::ostringstream oss;
		schema.toJson(oss);
		return oss.str();
	}
	catch (const avro::Exception& ex)
	{
		msg_err = "AVRO error: ";
		msg_err += ex.what();
		return std::string("");
	}
	catch (const std::exception& ex)
	{
		msg_err = "Error getting AVRO schema: ";
		msg_err += ex.what();
		return std::string("");
	}
}
