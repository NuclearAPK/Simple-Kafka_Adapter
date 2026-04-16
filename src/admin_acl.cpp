/*
 *  Simple Kafka 1C - Admin ACL Methods
 *  ACL management: create, describe, delete access control list entries
 */

#include "SimpleKafka1C.h"
#include "utils.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <algorithm>
#include <cctype>
#include <sstream>

//================================== Helpers ==========================================

namespace {

std::string toUpperCopy(const std::string& s)
{
	std::string r = s;
	std::transform(r.begin(), r.end(), r.begin(), [](unsigned char c){ return std::toupper(c); });
	return r;
}

bool parseResourceType(const std::string& s, rd_kafka_ResourceType_t& out, bool allowAny, std::string& errmsg)
{
	std::string v = toUpperCopy(s);
	if (v == "TOPIC")            { out = RD_KAFKA_RESOURCE_TOPIC;            return true; }
	if (v == "GROUP")            { out = RD_KAFKA_RESOURCE_GROUP;            return true; }
	if (v == "BROKER")           { out = RD_KAFKA_RESOURCE_BROKER;           return true; }
	if (v == "TRANSACTIONAL_ID") { out = RD_KAFKA_RESOURCE_TRANSACTIONAL_ID; return true; }
	if (allowAny && (v.empty() || v == "ANY")) { out = RD_KAFKA_RESOURCE_ANY; return true; }
	errmsg = "Invalid resource_type: '" + s + "'. Expected TOPIC, GROUP, BROKER, TRANSACTIONAL_ID" +
	         (allowAny ? " or ANY" : "");
	return false;
}

bool parsePatternType(const std::string& s, rd_kafka_ResourcePatternType_t& out, bool allowAny, std::string& errmsg)
{
	std::string v = toUpperCopy(s);
	if (v == "LITERAL")  { out = RD_KAFKA_RESOURCE_PATTERN_LITERAL;  return true; }
	if (v == "PREFIXED") { out = RD_KAFKA_RESOURCE_PATTERN_PREFIXED; return true; }
	if (allowAny)
	{
		if (v.empty() || v == "ANY") { out = RD_KAFKA_RESOURCE_PATTERN_ANY;   return true; }
		if (v == "MATCH")            { out = RD_KAFKA_RESOURCE_PATTERN_MATCH; return true; }
	}
	errmsg = "Invalid pattern_type: '" + s + "'. Expected LITERAL, PREFIXED" +
	         (allowAny ? ", MATCH or ANY" : "");
	return false;
}

bool parseAclOperation(const std::string& s, rd_kafka_AclOperation_t& out, bool allowAny, std::string& errmsg)
{
	std::string v = toUpperCopy(s);
	if (v == "ALL")               { out = RD_KAFKA_ACL_OPERATION_ALL;               return true; }
	if (v == "READ")              { out = RD_KAFKA_ACL_OPERATION_READ;              return true; }
	if (v == "WRITE")             { out = RD_KAFKA_ACL_OPERATION_WRITE;             return true; }
	if (v == "CREATE")            { out = RD_KAFKA_ACL_OPERATION_CREATE;            return true; }
	if (v == "DELETE")            { out = RD_KAFKA_ACL_OPERATION_DELETE;            return true; }
	if (v == "ALTER")             { out = RD_KAFKA_ACL_OPERATION_ALTER;             return true; }
	if (v == "DESCRIBE")          { out = RD_KAFKA_ACL_OPERATION_DESCRIBE;          return true; }
	if (v == "CLUSTER_ACTION")    { out = RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION;    return true; }
	if (v == "DESCRIBE_CONFIGS")  { out = RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS;  return true; }
	if (v == "ALTER_CONFIGS")     { out = RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS;     return true; }
	if (v == "IDEMPOTENT_WRITE")  { out = RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE;  return true; }
	if (allowAny && (v.empty() || v == "ANY")) { out = RD_KAFKA_ACL_OPERATION_ANY; return true; }
	errmsg = "Invalid operation: '" + s + "'";
	return false;
}

bool parsePermissionType(const std::string& s, rd_kafka_AclPermissionType_t& out, bool allowAny, std::string& errmsg)
{
	std::string v = toUpperCopy(s);
	if (v == "ALLOW") { out = RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW; return true; }
	if (v == "DENY")  { out = RD_KAFKA_ACL_PERMISSION_TYPE_DENY;  return true; }
	if (allowAny && (v.empty() || v == "ANY")) { out = RD_KAFKA_ACL_PERMISSION_TYPE_ANY; return true; }
	errmsg = "Invalid permission_type: '" + s + "'. Expected ALLOW, DENY" +
	         (allowAny ? " or ANY" : "");
	return false;
}

void aclBindingToJson(const rd_kafka_AclBinding_t* acl, boost::property_tree::ptree& node)
{
	const char* name = rd_kafka_AclBinding_name(acl);
	const char* principal = rd_kafka_AclBinding_principal(acl);
	const char* host = rd_kafka_AclBinding_host(acl);

	node.put("resource_type", rd_kafka_ResourceType_name(rd_kafka_AclBinding_restype(acl)));
	node.put("resource_name", name ? name : "");
	node.put("pattern_type", rd_kafka_ResourcePatternType_name(rd_kafka_AclBinding_resource_pattern_type(acl)));
	node.put("principal", principal ? principal : "");
	node.put("host", host ? host : "");
	node.put("operation", rd_kafka_AclOperation_name(rd_kafka_AclBinding_operation(acl)));
	node.put("permission_type", rd_kafka_AclPermissionType_name(rd_kafka_AclBinding_permission_type(acl)));
}

} // namespace

//================================== AddAcl ==========================================

bool SimpleKafka1C::addAcl(const variant_t& brokers, const variant_t& resourceType, const variant_t& resourceName,
                            const variant_t& patternType, const variant_t& principal, const variant_t& host,
                            const variant_t& operation, const variant_t& permissionType, const variant_t& timeout)
{
	char errstr[512];

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tResourceType = std::get<std::string>(resourceType);
	std::string tResourceName = std::get<std::string>(resourceName);
	std::string tPatternType = std::get<std::string>(patternType);
	std::string tPrincipal = std::get<std::string>(principal);
	std::string tHost = std::get<std::string>(host);
	std::string tOperation = std::get<std::string>(operation);
	std::string tPermissionType = std::get<std::string>(permissionType);
	int32_t tTimeout = std::get<int32_t>(timeout);

	if (!isValidBrokerList(tBrokers, msg_err))
		return false;
	if (tResourceName.empty())
	{
		msg_err = "resource_name must not be empty";
		return false;
	}
	if (tPrincipal.empty())
	{
		msg_err = "principal must not be empty (e.g. 'User:alice')";
		return false;
	}
	if (tHost.empty())
	{
		msg_err = "host must not be empty (use '*' for any)";
		return false;
	}

	rd_kafka_ResourceType_t restype;
	rd_kafka_ResourcePatternType_t ptype;
	rd_kafka_AclOperation_t op;
	rd_kafka_AclPermissionType_t perm;

	if (!parseResourceType(tResourceType, restype, false, msg_err)) return false;
	if (!parsePatternType(tPatternType, ptype, false, msg_err)) return false;
	if (!parseAclOperation(tOperation, op, false, msg_err)) return false;
	if (!parsePermissionType(tPermissionType, perm, false, msg_err)) return false;

	AdminClientScope admin(this, tBrokers, settings, RD_KAFKA_PRODUCER);
	if (!admin.isValid())
	{
		msg_err = admin.error();
		return false;
	}

	rd_kafka_AclBinding_t* acl = rd_kafka_AclBinding_new(
		restype, tResourceName.c_str(), ptype,
		tPrincipal.c_str(), tHost.c_str(), op, perm,
		errstr, sizeof(errstr));

	if (!acl)
	{
		msg_err = std::string("AclBinding creation failed: ") + errstr;
		return false;
	}

	rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(admin.get(), RD_KAFKA_ADMIN_OP_CREATEACLS);
	rd_kafka_AdminOptions_set_request_timeout(options, tTimeout, errstr, sizeof(errstr));

	rd_kafka_AclBinding_t* acl_arr[1] = { acl };
	rd_kafka_CreateAcls(admin.get(), acl_arr, 1, options, admin.queue());

	rd_kafka_event_t* rkev = rd_kafka_queue_poll(admin.queue(), tTimeout + 2000);

	bool success = false;
	if (rkev)
	{
		if (rd_kafka_event_error(rkev))
		{
			msg_err = rd_kafka_event_error_string(rkev);
		}
		else
		{
			const rd_kafka_CreateAcls_result_t* res = rd_kafka_event_CreateAcls_result(rkev);
			if (res)
			{
				size_t cnt;
				const rd_kafka_acl_result_t** results = rd_kafka_CreateAcls_result_acls(res, &cnt);
				if (cnt > 0)
				{
					const rd_kafka_error_t* err = rd_kafka_acl_result_error(results[0]);
					if (err)
					{
						const char* es = rd_kafka_error_string(err);
						msg_err = es ? es : "CreateAcls failed";
					}
					else
					{
						success = true;
					}
				}
				else
				{
					msg_err = "CreateAcls returned empty result";
				}
			}
		}
		rd_kafka_event_destroy(rkev);
	}
	else
	{
		msg_err = "Response timeout";
	}

	rd_kafka_AdminOptions_destroy(options);
	rd_kafka_AclBinding_destroy(acl);
	return success;
}

//================================== DescribeAcls ==========================================

std::string SimpleKafka1C::describeAcls(const variant_t& brokers, const variant_t& resourceType, const variant_t& resourceName,
                                         const variant_t& patternType, const variant_t& principal, const variant_t& host,
                                         const variant_t& operation, const variant_t& permissionType, const variant_t& timeout)
{
	std::string result;
	std::stringstream s{};
	char errstr[512];

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tResourceType = std::get<std::string>(resourceType);
	std::string tResourceName = std::get<std::string>(resourceName);
	std::string tPatternType = std::get<std::string>(patternType);
	std::string tPrincipal = std::get<std::string>(principal);
	std::string tHost = std::get<std::string>(host);
	std::string tOperation = std::get<std::string>(operation);
	std::string tPermissionType = std::get<std::string>(permissionType);
	int32_t tTimeout = std::get<int32_t>(timeout);

	if (!isValidBrokerList(tBrokers, msg_err))
		return result;

	rd_kafka_ResourceType_t restype;
	rd_kafka_ResourcePatternType_t ptype;
	rd_kafka_AclOperation_t op;
	rd_kafka_AclPermissionType_t perm;

	if (!parseResourceType(tResourceType, restype, true, msg_err)) return result;
	if (!parsePatternType(tPatternType, ptype, true, msg_err)) return result;
	if (!parseAclOperation(tOperation, op, true, msg_err)) return result;
	if (!parsePermissionType(tPermissionType, perm, true, msg_err)) return result;

	AdminClientScope admin(this, tBrokers, settings, RD_KAFKA_PRODUCER);
	if (!admin.isValid())
	{
		msg_err = admin.error();
		return result;
	}

	const char* namePtr = tResourceName.empty() ? nullptr : tResourceName.c_str();
	const char* principalPtr = tPrincipal.empty() ? nullptr : tPrincipal.c_str();
	const char* hostPtr = tHost.empty() ? nullptr : tHost.c_str();

	rd_kafka_AclBindingFilter_t* filter = rd_kafka_AclBindingFilter_new(
		restype, namePtr, ptype, principalPtr, hostPtr, op, perm,
		errstr, sizeof(errstr));

	if (!filter)
	{
		msg_err = std::string("AclBindingFilter creation failed: ") + errstr;
		return result;
	}

	rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(admin.get(), RD_KAFKA_ADMIN_OP_DESCRIBEACLS);
	rd_kafka_AdminOptions_set_operation_timeout(options, tTimeout, errstr, sizeof(errstr));

	rd_kafka_DescribeAcls(admin.get(), filter, options, admin.queue());

	rd_kafka_event_t* rkev = rd_kafka_queue_poll(admin.queue(), tTimeout + 2000);

	if (rkev)
	{
		if (rd_kafka_event_error(rkev))
		{
			msg_err = rd_kafka_event_error_string(rkev);
		}
		else
		{
			const rd_kafka_DescribeAcls_result_t* res = rd_kafka_event_DescribeAcls_result(rkev);
			if (res)
			{
				size_t cnt;
				const rd_kafka_AclBinding_t** acls = rd_kafka_DescribeAcls_result_acls(res, &cnt);

				boost::property_tree::ptree jsonObj;
				boost::property_tree::ptree aclChildren;

				for (size_t i = 0; i < cnt; i++)
				{
					boost::property_tree::ptree node;
					aclBindingToJson(acls[i], node);
					aclChildren.push_back(boost::property_tree::ptree::value_type("", node));
				}

				jsonObj.put("count", cnt);
				jsonObj.put_child("acls", aclChildren);
				boost::property_tree::write_json(s, jsonObj, true);
				result = s.str();
			}
		}
		rd_kafka_event_destroy(rkev);
	}
	else
	{
		msg_err = "Response timeout";
	}

	rd_kafka_AdminOptions_destroy(options);
	rd_kafka_AclBinding_destroy(filter);
	return result;
}

//================================== DeleteAcl ==========================================

std::string SimpleKafka1C::deleteAcl(const variant_t& brokers, const variant_t& resourceType, const variant_t& resourceName,
                                       const variant_t& patternType, const variant_t& principal, const variant_t& host,
                                       const variant_t& operation, const variant_t& permissionType, const variant_t& timeout)
{
	std::string result;
	std::stringstream s{};
	char errstr[512];

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tResourceType = std::get<std::string>(resourceType);
	std::string tResourceName = std::get<std::string>(resourceName);
	std::string tPatternType = std::get<std::string>(patternType);
	std::string tPrincipal = std::get<std::string>(principal);
	std::string tHost = std::get<std::string>(host);
	std::string tOperation = std::get<std::string>(operation);
	std::string tPermissionType = std::get<std::string>(permissionType);
	int32_t tTimeout = std::get<int32_t>(timeout);

	if (!isValidBrokerList(tBrokers, msg_err))
		return result;

	rd_kafka_ResourceType_t restype;
	rd_kafka_ResourcePatternType_t ptype;
	rd_kafka_AclOperation_t op;
	rd_kafka_AclPermissionType_t perm;

	if (!parseResourceType(tResourceType, restype, true, msg_err)) return result;
	if (!parsePatternType(tPatternType, ptype, true, msg_err)) return result;
	if (!parseAclOperation(tOperation, op, true, msg_err)) return result;
	if (!parsePermissionType(tPermissionType, perm, true, msg_err)) return result;

	AdminClientScope admin(this, tBrokers, settings, RD_KAFKA_PRODUCER);
	if (!admin.isValid())
	{
		msg_err = admin.error();
		return result;
	}

	const char* namePtr = tResourceName.empty() ? nullptr : tResourceName.c_str();
	const char* principalPtr = tPrincipal.empty() ? nullptr : tPrincipal.c_str();
	const char* hostPtr = tHost.empty() ? nullptr : tHost.c_str();

	rd_kafka_AclBindingFilter_t* filter = rd_kafka_AclBindingFilter_new(
		restype, namePtr, ptype, principalPtr, hostPtr, op, perm,
		errstr, sizeof(errstr));

	if (!filter)
	{
		msg_err = std::string("AclBindingFilter creation failed: ") + errstr;
		return result;
	}

	rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(admin.get(), RD_KAFKA_ADMIN_OP_DELETEACLS);
	rd_kafka_AdminOptions_set_operation_timeout(options, tTimeout, errstr, sizeof(errstr));

	rd_kafka_AclBindingFilter_t* filter_arr[1] = { filter };
	rd_kafka_DeleteAcls(admin.get(), filter_arr, 1, options, admin.queue());

	rd_kafka_event_t* rkev = rd_kafka_queue_poll(admin.queue(), tTimeout + 2000);

	if (rkev)
	{
		if (rd_kafka_event_error(rkev))
		{
			msg_err = rd_kafka_event_error_string(rkev);
		}
		else
		{
			const rd_kafka_DeleteAcls_result_t* res = rd_kafka_event_DeleteAcls_result(rkev);
			if (res)
			{
				size_t resp_cnt;
				const rd_kafka_DeleteAcls_result_response_t** responses =
					rd_kafka_DeleteAcls_result_responses(res, &resp_cnt);

				boost::property_tree::ptree jsonObj;
				boost::property_tree::ptree deletedChildren;
				size_t totalDeleted = 0;

				for (size_t i = 0; i < resp_cnt; i++)
				{
					const rd_kafka_error_t* rerr = rd_kafka_DeleteAcls_result_response_error(responses[i]);
					if (rerr)
					{
						const char* es = rd_kafka_error_string(rerr);
						msg_err = es ? es : "DeleteAcls failed";
						continue;
					}

					size_t match_cnt;
					const rd_kafka_AclBinding_t** matching =
						rd_kafka_DeleteAcls_result_response_matching_acls(responses[i], &match_cnt);

					for (size_t j = 0; j < match_cnt; j++)
					{
						boost::property_tree::ptree node;
						aclBindingToJson(matching[j], node);
						deletedChildren.push_back(boost::property_tree::ptree::value_type("", node));
						totalDeleted++;
					}
				}

				jsonObj.put("deleted_count", totalDeleted);
				jsonObj.put_child("deleted_acls", deletedChildren);
				boost::property_tree::write_json(s, jsonObj, true);
				result = s.str();
			}
		}
		rd_kafka_event_destroy(rkev);
	}
	else
	{
		msg_err = "Response timeout";
	}

	rd_kafka_AdminOptions_destroy(options);
	rd_kafka_AclBinding_destroy(filter);
	return result;
}
