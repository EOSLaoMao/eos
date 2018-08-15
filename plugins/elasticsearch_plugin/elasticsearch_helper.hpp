/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */

#pragma once
#include <vector>
#include <appbase/application.hpp>
#include <elasticlient/client.h>

namespace eosio {

class elasticsearch_helper
{
public:
   elasticsearch_helper(const std::vector<std::string> url_list, const std::string &index_name)
      :index_name(index_name), client(url_list){};

   void delete_index();
   void init_index(const std::string &mappings);
   void update(const std::string &type, const std::string &body);

   std::string index_name;
   elasticlient::Client client;
};

} // namespace
