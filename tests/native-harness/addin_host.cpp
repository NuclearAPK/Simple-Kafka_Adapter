#include "addin_host.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>

#ifdef _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#endif

std::u16string utf8ToUtf16(const std::string& src)
{
	std::u16string out;
	out.reserve(src.size());
	size_t i = 0;
	while (i < src.size())
	{
		const unsigned char c = static_cast<unsigned char>(src[i]);
		uint32_t cp = 0;
		size_t len = 0;
		if (c < 0x80) { cp = c; len = 1; }
		else if ((c >> 5) == 0x6) { cp = c & 0x1F; len = 2; }
		else if ((c >> 4) == 0xE) { cp = c & 0x0F; len = 3; }
		else if ((c >> 3) == 0x1E) { cp = c & 0x07; len = 4; }
		else { out.push_back(u'�'); ++i; continue; }
		if (i + len > src.size())
		{
			out.push_back(u'�');
			break;
		}
		for (size_t k = 1; k < len; ++k)
		{
			cp = (cp << 6) | (static_cast<unsigned char>(src[i + k]) & 0x3F);
		}
		i += len;
		if (cp >= 0x10000)
		{
			cp -= 0x10000;
			out.push_back(static_cast<char16_t>(0xD800 + (cp >> 10)));
			out.push_back(static_cast<char16_t>(0xDC00 + (cp & 0x3FF)));
		}
		else
		{
			out.push_back(static_cast<char16_t>(cp));
		}
	}
	return out;
}

std::string utf16ToUtf8(const char16_t* src, size_t len)
{
	std::string out;
	out.reserve(len);
	for (size_t i = 0; i < len; ++i)
	{
		uint32_t cp = src[i];
		if (cp >= 0xD800 && cp <= 0xDBFF && i + 1 < len && src[i + 1] >= 0xDC00 && src[i + 1] <= 0xDFFF)
		{
			cp = 0x10000 + ((cp - 0xD800) << 10) + (src[i + 1] - 0xDC00);
			++i;
		}
		if (cp < 0x80)
		{
			out.push_back(static_cast<char>(cp));
		}
		else if (cp < 0x800)
		{
			out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
			out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
		}
		else if (cp < 0x10000)
		{
			out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
			out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
			out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
		}
		else
		{
			out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
			out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
			out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
			out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
		}
	}
	return out;
}

static std::string fromWchar(const WCHAR_T* src)
{
	if (src == nullptr)
	{
		return {};
	}
	const char16_t* p = reinterpret_cast<const char16_t*>(src);
	size_t len = 0;
	while (p[len] != 0)
	{
		++len;
	}
	return utf16ToUtf8(p, len);
}

std::string valueToString(const Value& value)
{
	struct Visitor
	{
		std::string operator()(std::monostate) const { return "<empty>"; }
		std::string operator()(int32_t v) const { return std::to_string(v); }
		std::string operator()(int64_t v) const { return std::to_string(v); }
		std::string operator()(double v) const { return std::to_string(v); }
		std::string operator()(bool v) const { return v ? "true" : "false"; }
		std::string operator()(const std::string& v) const { return v; }
	};
	return std::visit(Visitor{}, value);
}

//---------------------------------------------------------------------------------------

bool MemoryManager::AllocMemory(void** pMemory, unsigned long ulCountByte)
{
	*pMemory = std::malloc(ulCountByte == 0 ? 1 : ulCountByte);
	return *pMemory != nullptr;
}

void MemoryManager::FreeMemory(void** pMemory)
{
	std::free(*pMemory);
	*pMemory = nullptr;
}

//---------------------------------------------------------------------------------------

bool Platform::AddError(unsigned short wcode, const WCHAR_T* source, const WCHAR_T* descr, long scode)
{
	std::printf("    [AddError] code=%u scode=%ld %s: %s\n", wcode, scode, fromWchar(source).c_str(), fromWchar(descr).c_str());
	return true;
}

bool Platform::Read(WCHAR_T*, tVariant*, long*, WCHAR_T**) { return false; }
bool Platform::Write(WCHAR_T*, tVariant*) { return false; }
bool Platform::RegisterProfileAs(WCHAR_T*) { return true; }

bool Platform::SetEventBufferDepth(long lDepth)
{
	std::lock_guard<std::mutex> lk(mtx);
	eventBufferDepth = lDepth;
	return true;
}

long Platform::GetEventBufferDepth()
{
	std::lock_guard<std::mutex> lk(mtx);
	return eventBufferDepth;
}

bool Platform::ExternalEvent(WCHAR_T* wszSource, WCHAR_T* wszMessage, WCHAR_T* wszData)
{
	std::lock_guard<std::mutex> lk(mtx);
	events.push_back({fromWchar(wszSource), fromWchar(wszMessage), fromWchar(wszData)});
	return true;
}

void Platform::CleanEventBuffer()
{
	std::lock_guard<std::mutex> lk(mtx);
	events.clear();
}

bool Platform::SetStatusLine(WCHAR_T*) { return true; }
void Platform::ResetStatusLine() {}

std::vector<ExternalEventRecord> Platform::takeEvents()
{
	std::lock_guard<std::mutex> lk(mtx);
	std::vector<ExternalEventRecord> out;
	out.swap(events);
	return out;
}

//---------------------------------------------------------------------------------------

Library::Library(const std::string& path)
{
#ifdef _WIN32
	const std::u16string wide = utf8ToUtf16(path);
	HMODULE h = LoadLibraryW(reinterpret_cast<const wchar_t*>(wide.c_str()));
	if (h == nullptr)
	{
		throw std::runtime_error("LoadLibrary failed for " + path + ", error " + std::to_string(GetLastError()));
	}
	module = h;
	getClassObject = reinterpret_cast<GetClassObjectPtr>(GetProcAddress(h, "GetClassObject"));
	destroyObject = reinterpret_cast<DestroyObjectPtr>(GetProcAddress(h, "DestroyObject"));
#else
	module = dlopen(path.c_str(), RTLD_NOW);
	if (module == nullptr)
	{
		throw std::runtime_error(std::string("dlopen failed: ") + dlerror());
	}
	getClassObject = reinterpret_cast<GetClassObjectPtr>(dlsym(module, "GetClassObject"));
	destroyObject = reinterpret_cast<DestroyObjectPtr>(dlsym(module, "DestroyObject"));
#endif
	if (getClassObject == nullptr || destroyObject == nullptr)
	{
		throw std::runtime_error("GetClassObject/DestroyObject not exported by " + path);
	}
}

Library::~Library()
{
#ifdef _WIN32
	if (module != nullptr) FreeLibrary(static_cast<HMODULE>(module));
#else
	if (module != nullptr) dlclose(module);
#endif
}

//---------------------------------------------------------------------------------------

AddIn::AddIn(Library& library_) : library(library_)
{
	static const std::u16string className = u"simpleKafka1C";
	if (library.getClassObject(reinterpret_cast<const WCHAR_T*>(className.c_str()), &component) == 0 || component == nullptr)
	{
		throw std::runtime_error("GetClassObject returned no object");
	}
	component->setMemManager(&memory);
	component->Init(&platform);
}

AddIn::~AddIn()
{
	if (component != nullptr)
	{
		component->Done();
		library.destroyObject(&component);
	}
}

void AddIn::toVariant(const Value& value, tVariant& dst)
{
	std::memset(&dst, 0, sizeof(dst));
	if (std::holds_alternative<std::monostate>(value))
	{
		dst.vt = VTYPE_EMPTY;
	}
	else if (const auto* i = std::get_if<int32_t>(&value))
	{
		dst.vt = VTYPE_I4;
		dst.lVal = *i;
	}
	else if (const auto* l = std::get_if<int64_t>(&value))
	{
		dst.vt = VTYPE_I8;
		dst.llVal = *l;
	}
	else if (const auto* d = std::get_if<double>(&value))
	{
		dst.vt = VTYPE_R8;
		dst.dblVal = *d;
	}
	else if (const auto* b = std::get_if<bool>(&value))
	{
		dst.vt = VTYPE_BOOL;
		dst.bVal = *b;
	}
	else
	{
		// Allocated through the memory manager, like platform strings
		const std::u16string wide = utf8ToUtf16(std::get<std::string>(value));
		const size_t bytes = (wide.size() + 1) * sizeof(char16_t);
		void* buf = nullptr;
		memory.AllocMemory(&buf, static_cast<unsigned long>(bytes));
		std::memcpy(buf, wide.c_str(), bytes);
		dst.vt = VTYPE_PWSTR;
		dst.pwstrVal = static_cast<WCHAR_T*>(buf);
		dst.wstrLen = static_cast<uint32_t>(wide.size());
	}
}

Value AddIn::fromVariant(tVariant& src)
{
	switch (src.vt)
	{
	case VTYPE_EMPTY:
		return std::monostate{};
	case VTYPE_I4:
		return src.lVal;
	case VTYPE_I8:
		return static_cast<int64_t>(src.llVal);
	case VTYPE_R8:
		return src.dblVal;
	case VTYPE_BOOL:
		return src.bVal;
	case VTYPE_PWSTR:
		return utf16ToUtf8(reinterpret_cast<const char16_t*>(src.pwstrVal), src.wstrLen);
	case VTYPE_BLOB:
		return std::string(src.pstrVal, src.strLen);
	default:
		throw std::runtime_error("unsupported result type " + std::to_string(src.vt));
	}
}

void AddIn::freeVariant(tVariant& var)
{
	if (var.vt == VTYPE_PWSTR && var.pwstrVal != nullptr)
	{
		memory.FreeMemory(reinterpret_cast<void**>(&var.pwstrVal));
	}
	else if ((var.vt == VTYPE_PSTR || var.vt == VTYPE_BLOB) && var.pstrVal != nullptr)
	{
		memory.FreeMemory(reinterpret_cast<void**>(&var.pstrVal));
	}
	var.vt = VTYPE_EMPTY;
}

Value AddIn::call(const std::string& name, const std::vector<Value>& args)
{
	const std::u16string wname = utf8ToUtf16(name);
	const long method = component->FindMethod(reinterpret_cast<const WCHAR_T*>(wname.c_str()));
	if (method < 0)
	{
		throw std::runtime_error("method not found: " + name);
	}
	const long count = component->GetNParams(method);
	if (static_cast<long>(args.size()) > count)
	{
		throw std::runtime_error(name + ": too many arguments");
	}

	std::vector<tVariant> params(static_cast<size_t>(count));
	for (long i = 0; i < count; ++i)
	{
		tVariant& p = params[static_cast<size_t>(i)];
		if (i < static_cast<long>(args.size()))
		{
			toVariant(args[static_cast<size_t>(i)], p);
		}
		else
		{
			std::memset(&p, 0, sizeof(p));
			if (!component->GetParamDefValue(method, i, &p))
			{
				for (auto& q : params) freeVariant(q);
				throw std::runtime_error(name + ": no value for parameter " + std::to_string(i));
			}
		}
	}

	Value result;
	tVariant ret;
	std::memset(&ret, 0, sizeof(ret));
	if (component->HasRetVal(method))
	{
		component->CallAsFunc(method, &ret, params.data(), count);
		result = fromVariant(ret);
		freeVariant(ret);
	}
	else
	{
		component->CallAsProc(method, params.data(), count);
	}
	for (auto& p : params)
	{
		freeVariant(p);
	}
	return result;
}

bool AddIn::callBool(const std::string& name, const std::vector<Value>& args)
{
	const Value v = call(name, args);
	if (const auto* b = std::get_if<bool>(&v))
	{
		return *b;
	}
	throw std::runtime_error(name + ": expected Boolean, got " + valueToString(v));
}

std::string AddIn::callString(const std::string& name, const std::vector<Value>& args)
{
	const Value v = call(name, args);
	if (const auto* s = std::get_if<std::string>(&v))
	{
		return *s;
	}
	throw std::runtime_error(name + ": expected String, got " + valueToString(v));
}

std::string AddIn::lastError()
{
	return callString("ПолучитьСообщениеОбОшибке");
}
