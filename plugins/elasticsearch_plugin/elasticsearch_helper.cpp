#include "elasticsearch_helper.hpp"
#include <cpr/response.h>

#include <fc/io/json.hpp>
#include <fc/log/logger.hpp>

#include <boost/format.hpp>

#include <eosio/chain/exceptions.hpp>

namespace eosio {

namespace {
   bool is_2xx(int32_t status_code)
   {
      return status_code > 199 && status_code < 300;
   }
}

void elasticsearch_helper::index(const std::string &type, const std::string &body)
{
   cpr::Response resp = client.index(index_name, type, "", body);
   EOS_ASSERT( is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text) );
}

void elasticsearch_helper::init_index(const std::string &mappings)
{
   cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::PUT, index_name, mappings);
   EOS_ASSERT( is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text) );
}

void elasticsearch_helper::delete_index()
{
   cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::DELETE, index_name, "");
   EOS_ASSERT( is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text) );
}

uint64_t elasticsearch_helper::count_doc(const std::string &type, const std::string &query)
{
   auto url = boost::str(boost::format("%1%/%2%/_count") % index_name % type);
   cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::GET, url, query);
   EOS_ASSERT( is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text) );
   auto v = fc::json::from_string(resp.text);
   return v["count"].as_uint64();
}

void elasticsearch_helper::search(fc::variant& v, const std::string &type, const std::string &query)
{
   cpr::Response resp = client.search(index_name, type, query);
   EOS_ASSERT( is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text) );
   v = fc::json::from_string(resp.text);
}

}
