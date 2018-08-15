#include "elasticsearch_helper.hpp"
#include <cpr/response.h>
#include <fc/log/logger.hpp>
#include <eosio/chain/exceptions.hpp>

namespace eosio {

void elasticsearch_helper::update(const std::string &type, const std::string &body)
{
   try {
      cpr::Response resp = client.index(index_name, type, "", body);
      EOS_ASSERT( resp.status_code == 200, chain::response_code_exception, "${text}", ("text", resp.text) );
   } catch(elasticlient::ConnectionException) {
      EOS_THROW( chain::elastic_connection_exception, "" )
   }
}

void elasticsearch_helper::init_index(const std::string &mappings)
{
   try {
      cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::PUT, index_name, mappings);
      EOS_ASSERT( resp.status_code == 200, chain::response_code_exception, "${text}", ("text", resp.text) );
   } catch(elasticlient::ConnectionException) {
      EOS_THROW( chain::elastic_connection_exception, "" )
   }

}

void elasticsearch_helper::delete_index()
{
   try {
      cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::DELETE, index_name, "");
      EOS_ASSERT( resp.status_code == 200, chain::response_code_exception, "${text}", ("text", resp.text) );
   } catch(elasticlient::ConnectionException) {
      EOS_THROW( chain::elastic_connection_exception, "" )
   }

}

}