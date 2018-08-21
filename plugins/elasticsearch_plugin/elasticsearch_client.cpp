#include "elasticsearch_client.hpp"
#include <cpr/response.h>

#include <fc/io/json.hpp>
#include <fc/log/logger.hpp>

#include <boost/format.hpp>

#include <eosio/chain/exceptions.hpp>

namespace eosio
{

namespace
{
bool is_2xx(int32_t status_code)
{
   return status_code > 199 && status_code < 300;
}
} // namespace

void elasticsearch_client::index(const std::string &type, const std::string &body, const std::string &id)
{
   cpr::Response resp = client.index(index_name, type, id, body);
   EOS_ASSERT(is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
}

void elasticsearch_client::init_index(const std::string &mappings)
{
   cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::PUT, index_name, mappings);
   EOS_ASSERT(is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
}

void elasticsearch_client::delete_index()
{
   // retrn status code 404 if index not exists
   client.performRequest(elasticlient::Client::HTTPMethod::DELETE, index_name, "");
}

uint64_t elasticsearch_client::count_doc(const std::string &type, const std::string &query)
{
   auto url = boost::str(boost::format("%1%/%2%/_count") % index_name % type);
   cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::GET, url, query);
   EOS_ASSERT(is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
   auto v = fc::json::from_string(resp.text);
   return v["count"].as_uint64();
}

void elasticsearch_client::search(fc::variant &v, const std::string &type, const std::string &query)
{
   cpr::Response resp = client.search(index_name, type, query);
   EOS_ASSERT(is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
   v = fc::json::from_string(resp.text);
}

void elasticsearch_client::delete_by_query(const std::string &type, const std::string &query)
{
   auto url = boost::str(boost::format("%1%/%2%/_delete_by_query") % index_name % type);
   cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::POST, url, query);
   EOS_ASSERT(is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
}

void elasticsearch_client::bulk_perform(elasticlient::SameIndexBulkData &bulk)
{
   size_t errors = bulk_indexer.perform(bulk);
   EOS_ASSERT(errors == 0, chain::bulk_fail_exception, "bulk perform error num: ${errors}", ("errors", errors));
}

} // namespace eosio
