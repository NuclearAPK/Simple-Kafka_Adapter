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

#ifndef COMPONENT_H
#define COMPONENT_H

#include <ctime>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "ComponentBase.h"
#include "AddInDefBase.h"
#include "IMemoryManager.h"
//#include <types.h>

template<class... Ts>
struct overloaded : Ts ... {
    using Ts::operator()...;
};
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

#define UNDEFINED std::monostate()

typedef std::variant<
        std::monostate,
        int32_t,
        //int64_t,
        double,
        bool,
        std::string,
        std::tm,
        std::vector<char>        
> variant_t;

class Component : public IComponentBase {
public:
    // IInitDoneBase
    virtual bool ADDIN_API Init(void*) override;
    virtual bool ADDIN_API setMemManager(void* mem) override;
    virtual long ADDIN_API GetInfo() override { return 2100; }
    virtual void ADDIN_API Done() override {}
    // ILanguageExtenderBase
    virtual bool ADDIN_API RegisterExtensionAs(WCHAR_T**) override;
    virtual long ADDIN_API GetNProps() override;
    virtual long ADDIN_API FindProp(const WCHAR_T* wsPropName) override;
    virtual const WCHAR_T* ADDIN_API GetPropName(long lPropNum, long lPropAlias) override;
    virtual bool ADDIN_API GetPropVal(const long lPropNum, tVariant* pvarPropVal) override;
    virtual bool ADDIN_API SetPropVal(const long lPropNum, tVariant* varPropVal) override;
    virtual bool ADDIN_API IsPropReadable(const long lPropNum) override;
    virtual bool ADDIN_API IsPropWritable(const long lPropNum) override;
    virtual long ADDIN_API GetNMethods() override;
    virtual long ADDIN_API FindMethod(const WCHAR_T* wsMethodName) override;
    virtual const WCHAR_T* ADDIN_API GetMethodName(const long lMethodNum, 
                            const long lMethodAlias) override;
    virtual long ADDIN_API GetNParams(const long lMethodNum) override;
    virtual bool ADDIN_API GetParamDefValue(const long lMethodNum, const long lParamNum,
                            tVariant *pvarParamDefValue) override;   
    virtual bool ADDIN_API HasRetVal(const long lMethodNum) override;
    virtual bool ADDIN_API CallAsProc(const long lMethodNum,
                    tVariant* paParams, const long lSizeArray) override;
    virtual bool ADDIN_API CallAsFunc(const long lMethodNum,
                tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray) override;
    // LocaleBase
    virtual void ADDIN_API SetLocale(const WCHAR_T* loc) override;
    // UserLanguageBase
    virtual void ADDIN_API SetUserInterfaceLanguageCode(const WCHAR_T* lang) override;

protected:
	std::string msg_err;

    virtual std::string extensionName() = 0;

    bool AddError(unsigned short code, const std::string &src, const std::string &msg, long scode);

    bool ExternalEvent(const std::string &src, const std::string &msg, const std::string &data);

    bool SetEventBufferDepth(long depth);

    long GetEventBufferDepth();

    void AddProperty(const std::wstring &alias, const std::wstring &alias_ru,
                     std::function<std::shared_ptr<variant_t>(void)> getter = nullptr,
                     std::function<void(variant_t &&)> setter = nullptr);

    void AddProperty(const std::wstring &alias, const std::wstring &alias_ru,
                     std::shared_ptr<variant_t> storage);

    template<typename T, typename C, typename ... Ts>
    void AddMethod(const std::wstring &alias, const std::wstring &alias_ru, C *c, T(C::*f)(Ts ...),
                   std::map<long, variant_t> &&def_args = {});

private:
    static constexpr char UNKNOWN_EXCP[] = u8"Unknown unhandled exception";
    class PropertyMeta;

    class MethodMeta;

    template<size_t... Indices>
    static auto refTupleGen(std::vector<variant_t> &v, std::index_sequence<Indices...>);

    static variant_t toStlVariant(tVariant src);

    static std::string toUTF8String(std::basic_string_view<WCHAR_T> src);

    static std::wstring toWstring(std::basic_string_view<WCHAR_T> src);

    static std::u16string toUTF16String(std::string_view src);

    void storeVariable(const std::string &src, tVariant &dst);

    void storeVariable(const std::string &src, WCHAR_T **dst);

    void storeVariable(const std::u16string &src, WCHAR_T **dst);

    void storeVariable(const std::vector<char> &src, tVariant &dst);

    void storeVariable(const variant_t &src, tVariant &dst);

    static std::vector<variant_t> parseParams(tVariant *params, long array_size);

    void storeParams(const std::vector<variant_t> &src, tVariant *dest);

    static std::wstring toUpper(std::wstring str);

    std::u16string m_userLang;
    IAddInDefBase *connection;
    IMemoryManager *memory_manager;
    std::vector<PropertyMeta> properties_meta;
    std::vector<MethodMeta> methods_meta;

};

class Component::PropertyMeta {
public:
    PropertyMeta &operator=(const PropertyMeta &) = delete;

    PropertyMeta(const PropertyMeta &) = delete;

    PropertyMeta(PropertyMeta &&) = default;

    PropertyMeta &operator=(PropertyMeta &&) = default;

    std::wstring alias;
    std::wstring alias_ru;
    std::function<std::shared_ptr<variant_t>(void)> getter;
    std::function<void(variant_t &&)> setter;
};

class Component::MethodMeta {
public:
    MethodMeta &operator=(const MethodMeta &) = delete;

    MethodMeta(const MethodMeta &) = delete;

    MethodMeta(MethodMeta &&) = default;

    MethodMeta &operator=(MethodMeta &&) = default;

    std::wstring alias;
    std::wstring alias_ru;
    long params_count;
    bool returns_value;
    bool returns_bool;
    bool returns_int32_t;
    std::map<long, variant_t> default_args;
    std::function<variant_t(std::vector<variant_t> &params)> call;
};

template<size_t... Indices>
auto Component::refTupleGen(std::vector<variant_t> &v, std::index_sequence<Indices...>) {
    return std::forward_as_tuple(v[Indices]...);
}

template<typename T, typename C, typename ... Ts>
void Component::AddMethod(const std::wstring &alias, const std::wstring &alias_ru, C *c, T(C::*f)(Ts ...),
                          std::map<long, variant_t> &&def_args) {

    MethodMeta meta{alias, alias_ru, sizeof...(Ts), 
                    !std::is_same<T, void>::value, 
                    !std::is_same<T, bool>::value, 
                    !std::is_same<T, int32_t>::value, 
                    std::move(def_args),
                    [f, c](std::vector<variant_t> &params) -> variant_t {
                        auto args = refTupleGen(params, std::make_index_sequence<sizeof...(Ts)>());
                        if constexpr (std::is_same<T, void>::value) {
                            std::apply(f, std::tuple_cat(std::make_tuple(c), args));
                            return UNDEFINED;
                        } else {
                            return std::apply(f, std::tuple_cat(std::make_tuple(c), args));
                        }
                    }
    };

    methods_meta.push_back(std::move(meta));
};

#endif //COMPONENT_H
