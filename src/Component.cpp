/*
 *  Modern Native AddIn
 *  Copyright (C) 2018  Infactum
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

#include <algorithm>
#include <codecvt>
#include <cwctype>
#include <locale>

#include "Component.h"

#ifdef _WINDOWS
#pragma warning (disable : 4267)
#endif

bool Component::Init(void *connection_) {
    connection = static_cast<IAddInDefBase *>(connection_);
    return connection != nullptr;
}

bool Component::setMemManager(void *memory_manager_) {
    memory_manager = static_cast<IMemoryManager *>(memory_manager_);
    return memory_manager != nullptr;
}

void Component::SetLocale(const WCHAR_T *locale) {
#ifdef CASE_INSENSITIVE
    try {
        std::locale::global(std::locale{toUTF8String(locale)});
    } catch (std::runtime_error &) {
        std::locale::global(std::locale{""});
    }
#endif
}

bool Component::RegisterExtensionAs(WCHAR_T **ext_name) {
    auto name = extensionName();

    try {
        storeVariable(name, ext_name);
    } catch (const std::bad_alloc &) {
        return false;
    }

    return true;
}

long Component::GetNProps() {
    return properties_meta.size();
}

long Component::FindProp(const WCHAR_T *prop_name) {

    std::wstring lookup_name = toWstring(prop_name);

#ifdef CASE_INSENSITIVE
    lookup_name = toUpper(lookup_name);

    for (auto i = 0u; i < properties_meta.size(); ++i) {
        if (toUpper(properties_meta[i].alias) == lookup_name
            || toUpper(properties_meta[i].alias_ru) == lookup_name) {
            return static_cast<long>(i);
        }
    }
#else
    for (auto i = 0u; i < properties_meta.size(); ++i) {
        if (properties_meta[i].alias == lookup_name
            || properties_meta[i].alias_ru == lookup_name) {
            return static_cast<long>(i);
        }
    }
#endif
    return -1;

}

const WCHAR_T *Component::GetPropName(long num, long lang_alias) {

    std::wstring *name;
    if (lang_alias == 0) {
        name = &(properties_meta[num].alias);
    } else {
        name = &(properties_meta[num].alias_ru);
    }

    WCHAR_T *result = nullptr;
    storeVariable(std::u16string(reinterpret_cast<const char16_t *>(name->c_str())), &result);

    return result;
}

bool Component::GetPropVal(const long num, tVariant *value) {

    try {
        auto tmp = properties_meta[num].getter();
        storeVariable(*tmp, *value);
    } catch (const std::exception &e) {
        AddError(ADDIN_E_FAIL, extensionName(), e.what(), true);
        return false;
    } catch (...) {
        AddError(ADDIN_E_FAIL, extensionName(), UNKNOWN_EXCP, true);
        return false;
    }

    return true;
}

bool Component::SetPropVal(const long num, tVariant *value) {

    try {
        auto tmp = toStlVariant(*value);
        properties_meta[num].setter(std::move(tmp));
    } catch (const std::exception &e) {
        AddError(ADDIN_E_FAIL, extensionName(), e.what(), true);
        return false;
    } catch (...) {
        AddError(ADDIN_E_FAIL, extensionName(), UNKNOWN_EXCP, true);
        return false;
    }

    return true;
}

bool Component::IsPropReadable(const long lPropNum) {
    return static_cast<bool>(properties_meta[lPropNum].getter);
}

bool Component::IsPropWritable(const long lPropNum) {
    return static_cast<bool>(properties_meta[lPropNum].setter);
}

long Component::GetNMethods() {
    return methods_meta.size();
}

long Component::FindMethod(const WCHAR_T *method_name) {

    std::wstring lookup_name = toWstring(method_name);

#ifdef CASE_INSENSITIVE
    lookup_name = toUpper(lookup_name);

    for (auto i = 0u; i < methods_meta.size(); ++i) {
        if (toUpper(methods_meta[i].alias) == lookup_name
            || toUpper(methods_meta[i].alias_ru) == lookup_name) {
            return static_cast<long>(i);
        }
    }
#else
    for (auto i = 0u; i < methods_meta.size(); ++i) {
        if (methods_meta[i].alias == lookup_name
            || methods_meta[i].alias_ru == lookup_name) {
            return static_cast<long>(i);
        }
    }
#endif

    return -1;
}

const WCHAR_T *Component::GetMethodName(const long num, const long lang_alias) {

    std::wstring *name;
    if (lang_alias == 0) {
        name = &(methods_meta[num].alias);
    } else {
        name = &(methods_meta[num].alias_ru);
    }

    WCHAR_T *result = nullptr;
    storeVariable(std::u16string(reinterpret_cast<const char16_t *>(name->c_str())), &result);

    return result;

}

long Component::GetNParams(const long method_num) {
    return methods_meta[method_num].params_count;
}

bool Component::GetParamDefValue(const long method_num, const long param_num, tVariant *def_value) {

    auto &def_args = methods_meta[method_num].default_args;

    auto it = def_args.find(param_num);
    if (it == def_args.end()) {
        return false;
    }

    storeVariable(it->second, *def_value);
    return true;
}

bool Component::HasRetVal(const long method_num) {
    return methods_meta[method_num].returns_value;
}

bool Component::CallAsProc(const long method_num, tVariant *params, const long array_size) {

    try {
        auto args = parseParams(params, array_size);
        methods_meta[method_num].call(args);
#ifdef OUT_PARAMS
        storeParams(args, params);
#endif
    } catch (const std::exception &e) {
        AddError(ADDIN_E_FAIL, extensionName(), e.what(), true);
        return false;
    } catch (...) {
        AddError(ADDIN_E_FAIL, extensionName(), UNKNOWN_EXCP, true);
        return false;
    }

    return true;
}

bool Component::CallAsFunc(const long method_num, tVariant *ret_value, tVariant *params, const long array_size) {

    try {
        auto args = parseParams(params, array_size);
        variant_t result = methods_meta[method_num].call(args);
        storeVariable(result, *ret_value);
#ifdef OUT_PARAMS
        storeParams(args, params);
#endif
    } catch (const std::exception &e) {
        AddError(ADDIN_E_FAIL, extensionName(), e.what(), true);
        return false;
    } catch (...) {
        AddError(ADDIN_E_FAIL, extensionName(), UNKNOWN_EXCP, true);
        return false;
    }

    return true;

}

void Component::AddError(unsigned short code, const std::string &src, const std::string &msg, bool throw_excp) {
    WCHAR_T *source = nullptr;
    WCHAR_T *descr = nullptr;

    storeVariable(src, &source);
    storeVariable(msg, &descr);

    connection->AddError(code, source, descr, throw_excp);

    memory_manager->FreeMemory(reinterpret_cast<void **>(&source));
    memory_manager->FreeMemory(reinterpret_cast<void **>(&descr));
}

bool Component::ExternalEvent(const std::string &src, const std::string &msg, const std::string &data) {
    WCHAR_T *wszSource = nullptr;
    WCHAR_T *wszMessage = nullptr;
    WCHAR_T *wszData = nullptr;

    storeVariable(src, &wszSource);
    storeVariable(msg, &wszMessage);
    storeVariable(data, &wszData);

    auto success = connection->ExternalEvent(wszSource, wszMessage, wszData);

    memory_manager->FreeMemory(reinterpret_cast<void **>(&wszSource));
    memory_manager->FreeMemory(reinterpret_cast<void **>(&wszMessage));
    memory_manager->FreeMemory(reinterpret_cast<void **>(&wszData));

    return success;
}

bool Component::SetEventBufferDepth(long depth) {
    return connection->SetEventBufferDepth(depth);
}

long Component::GetEventBufferDepth() {
    return connection->GetEventBufferDepth();
}

void Component::AddProperty(const std::wstring &alias, const std::wstring &alias_ru,
                            std::function<std::shared_ptr<variant_t>(void)> getter,
                            std::function<void(variant_t &&)> setter) {

    PropertyMeta meta{alias, alias_ru, std::move(getter), std::move(setter)};
    properties_meta.push_back(std::move(meta));

}

void Component::AddProperty(const std::wstring &alias, const std::wstring &alias_ru,
                            std::shared_ptr<variant_t> storage) {

    if (!storage) {
        return;
    }

    AddProperty(alias, alias_ru,
                [storage]() { // getter
                    return storage;
                },
                [storage](variant_t &&v) -> void { //setter
                    *storage = std::move(v);
                });

}

variant_t Component::toStlVariant(tVariant src) {
    switch (src.vt) {
        case VTYPE_EMPTY:
            return UNDEFINED;
        case VTYPE_I4: //int32_t
            return src.lVal;
        case VTYPE_R8: //double
            return src.dblVal;
        case VTYPE_PWSTR: { //std::string
            return toUTF8String(std::basic_string_view(src.pwstrVal, src.wstrLen));
        }
        case VTYPE_BOOL:
            return src.bVal;
        case VTYPE_BLOB:
            return std::vector<char>(src.pstrVal, src.pstrVal + src.strLen);
        case VTYPE_TM:
            return src.tmVal;
        default:
            throw std::bad_cast();
    }
}

void Component::storeVariable(const variant_t &src, tVariant &dst) {

    if (dst.vt == VTYPE_PWSTR && dst.pwstrVal != nullptr) {
        memory_manager->FreeMemory(reinterpret_cast<void **>(&dst.pwstrVal));
    }

    if ((dst.vt == VTYPE_PSTR || dst.vt == VTYPE_BLOB) && dst.pstrVal != nullptr) {
        memory_manager->FreeMemory(reinterpret_cast<void **>(&dst.pstrVal));
    }

    std::visit(overloaded{
            [&](std::monostate) { dst.vt = VTYPE_EMPTY; },
            [&](const int32_t &v) {
                dst.vt = VTYPE_I4;
                dst.lVal = v;
            },
            [&](const double &v) {
                dst.vt = VTYPE_R8;
                dst.dblVal = v;
            },
            [&](const bool v) {
                dst.vt = VTYPE_BOOL;
                dst.bVal = v;
            },
            [&](const std::tm &v) {
                dst.vt = VTYPE_TM;
                dst.tmVal = v;
            },
            [&](const std::string &v) { storeVariable(v, dst); },
            [&](const std::vector<char> &v) { storeVariable(v, dst); }
    }, src);

}

void Component::storeVariable(const std::string &src, tVariant &dst) {

    std::u16string tmp = toUTF16String(src);

    dst.vt = VTYPE_PWSTR;
    storeVariable(tmp, &dst.pwstrVal);
    dst.wstrLen = static_cast<uint32_t>(tmp.length());
}

void Component::storeVariable(const std::string &src, WCHAR_T **dst) {
    std::u16string tmp = toUTF16String(src);
    storeVariable(tmp, dst);
}

void Component::storeVariable(const std::u16string &src, WCHAR_T **dst) {

    size_t c_size = (src.size() + 1) * sizeof(char16_t);

    if (!memory_manager || !memory_manager->AllocMemory(reinterpret_cast<void **>(dst), c_size)) {
        throw std::bad_alloc();
    };

    memcpy(*dst, src.c_str(), c_size);
}

void Component::storeVariable(const std::vector<char> &src, tVariant &dst) {

    dst.vt = VTYPE_BLOB;
    dst.strLen = src.size();

    if (!memory_manager || !memory_manager->AllocMemory(reinterpret_cast<void **>(&dst.pstrVal), src.size())) {
        throw std::bad_alloc();
    };

    memcpy(dst.pstrVal, src.data(), src.size());
}

std::vector<variant_t> Component::parseParams(tVariant *params, long array_size) {
    std::vector<variant_t> result;

    auto size = static_cast<const unsigned long>(array_size);
    result.reserve(size);
    for (size_t i = 0; i < size; i++) {
        result.emplace_back(toStlVariant(params[i]));
    }

    return result;
}

void Component::storeParams(const std::vector<variant_t> &src, tVariant *dest) {
    for (size_t i = 0; i < src.size(); i++) {
        storeVariable(src[i], dest[i]);
    }
}

std::wstring Component::toUpper(std::wstring str) {
    std::transform(str.begin(), str.end(), str.begin(), std::towupper);
    return str;
}

std::string Component::toUTF8String(std::basic_string_view<WCHAR_T> src) {
#ifdef _WINDOWS
    // VS bug
    // https://social.msdn.microsoft.com/Forums/en-US/8f40dcd8-c67f-4eba-9134-a19b9178e481/vs-2015-rc-linker-stdcodecvt-error?forum=vcgeneral
    static std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> cvt_utf8_utf16;
    return cvt_utf8_utf16.to_bytes(src.data(), src.data() + src.size());
#else
    static std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> cvt_utf8_utf16;
    return cvt_utf8_utf16.to_bytes(reinterpret_cast<const char16_t *>(src.data()),
                                   reinterpret_cast<const char16_t *>(src.data() + src.size()));
#endif
}

std::wstring Component::toWstring(std::basic_string_view<WCHAR_T> src) {
#ifdef _WINDOWS
    return std::wstring(src);
#else
    std::wstring_convert<std::codecvt_utf16<wchar_t, 0x10ffff, std::little_endian>> conv;
    return conv.from_bytes(reinterpret_cast<const char*>(src.data()),
                           reinterpret_cast<const char*>(src.data() + src.size()));
#endif
}

std::u16string Component::toUTF16String(std::string_view src) {
#ifdef _WINDOWS
    static std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> cvt_utf8_utf16;
    std::wstring tmp = cvt_utf8_utf16.from_bytes(src.data(), src.data() + src.size());
    return std::u16string(reinterpret_cast<const char16_t *>(tmp.data()), tmp.size());
#else
    static std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> cvt_utf8_utf16;
    return cvt_utf8_utf16.from_bytes(src.data(), src.data() + src.size());
#endif
}
