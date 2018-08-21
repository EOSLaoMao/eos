#pragma once
#include <vector>
#include <appbase/application.hpp>
#include <fc/variant.hpp>
#include <elasticlient/client.h>
#include <elasticlient/bulk.h>

namespace eosio {

class elasticsearch_client
{
public:
   elasticsearch_client(const std::vector<std::string> url_list, const std::string &index_name)
      :index_name(index_name), client(url_list), bulk_indexer(url_list, 6000){};

   void delete_index();
   void init_index(const std::string &mappings);
   void index(const std::string &type, const std::string &body, const std::string &id = "");
   uint64_t count_doc(const std::string &type, const std::string &query = std::string());
   void search(fc::variant& v, const std::string &type, const std::string &query);
   void delete_by_query(const std::string &type, const std::string &query);
   void bulk_perform(elasticlient::SameIndexBulkData &bulk);

   std::string index_name;
   elasticlient::Client client;
   elasticlient::Bulk bulk_indexer;
};

}
