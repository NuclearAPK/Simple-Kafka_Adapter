/*
 *  Native API host for tests: loads SimpleKafka1C.dll the way the 1C platform does
 *  (GetClassObject + IMemoryManager + IAddInDefBase) and calls methods by name.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

#include "ComponentBase.h"
#include "AddInDefBase.h"
#include "IMemoryManager.h"

// Argument and result of a component call. int64_t is sent as VTYPE_I8, the way the
// platform passes integers that do not fit int32.
using Value = std::variant<std::monostate, int32_t, int64_t, double, bool, std::string>;

std::u16string utf8ToUtf16(const std::string& src);
std::string utf16ToUtf8(const char16_t* src, size_t len);

std::string valueToString(const Value& value);

class MemoryManager : public IMemoryManager
{
public:
	bool ADDIN_API AllocMemory(void** pMemory, unsigned long ulCountByte) override;
	void ADDIN_API FreeMemory(void** pMemory) override;
};

struct ExternalEventRecord
{
	std::string source;
	std::string message;
	std::string data;
};

// Platform side of the connection: errors are printed, external events are stored.
class Platform : public IAddInDefBase
{
public:
	bool ADDIN_API AddError(unsigned short wcode, const WCHAR_T* source, const WCHAR_T* descr, long scode) override;
	bool ADDIN_API Read(WCHAR_T* wszPropName, tVariant* pVal, long* pErrCode, WCHAR_T** errDescriptor) override;
	bool ADDIN_API Write(WCHAR_T* wszPropName, tVariant* pVar) override;
	bool ADDIN_API RegisterProfileAs(WCHAR_T* wszProfileName) override;
	bool ADDIN_API SetEventBufferDepth(long lDepth) override;
	long ADDIN_API GetEventBufferDepth() override;
	bool ADDIN_API ExternalEvent(WCHAR_T* wszSource, WCHAR_T* wszMessage, WCHAR_T* wszData) override;
	void ADDIN_API CleanEventBuffer() override;
	bool ADDIN_API SetStatusLine(WCHAR_T* wszStatusLine) override;
	void ADDIN_API ResetStatusLine() override;

	std::vector<ExternalEventRecord> takeEvents();

private:
	std::mutex mtx;
	std::vector<ExternalEventRecord> events;
	long eventBufferDepth = 1;
};

class Library
{
public:
	explicit Library(const std::string& path);
	~Library();
	Library(const Library&) = delete;
	Library& operator=(const Library&) = delete;

	GetClassObjectPtr getClassObject = nullptr;
	DestroyObjectPtr destroyObject = nullptr;

private:
	void* module = nullptr;
};

// One component object ("Новый("AddIn.SimpleKafka.simpleKafka1C")" in 1C).
class AddIn
{
public:
	explicit AddIn(Library& library);
	~AddIn();
	AddIn(const AddIn&) = delete;
	AddIn& operator=(const AddIn&) = delete;

	// Calls a method by its Russian or English name. Missing trailing arguments are
	// filled from GetParamDefValue, as the platform does.
	Value call(const std::string& name, const std::vector<Value>& args = {});

	bool callBool(const std::string& name, const std::vector<Value>& args = {});
	std::string callString(const std::string& name, const std::vector<Value>& args = {});

	std::string lastError();

	Platform platform;

private:
	Library& library;
	MemoryManager memory;
	IComponentBase* component = nullptr;

	void toVariant(const Value& value, tVariant& dst);
	Value fromVariant(tVariant& src);
	void freeVariant(tVariant& var);
};
