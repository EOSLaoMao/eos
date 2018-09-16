/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
#include <fc/variant.hpp>
#include <fc/io/json.hpp>
#include <eosio/chain/controller.hpp>
#include <eosio/blacklist_plugin/blacklist_plugin.hpp>

namespace eosio {

   static appbase::abstract_plugin& _template_plugin = app().register_plugin<blacklist_plugin>();

   using namespace eosio;

    #define CALL(api_name, api_handle, call_name, INVOKE, http_response_code) \
    {std::string("/v1/" #api_name "/" #call_name), \
    [api_handle](string, string body, url_response_callback cb) mutable { \
          try { \
             if (body.empty()) body = "{}"; \
             INVOKE \
             cb(http_response_code, fc::json::to_string(result)); \
          } catch (...) { \
             http_plugin::handle_exception(#api_name, #call_name, body, cb); \
          } \
       }}

    #define INVOKE_R_V(api_handle, call_name) \
    auto result = api_handle->call_name();

   template <class Container, class Function>
   auto apply (const Container &cont, Function fun) {
       std::vector< typename
               std::result_of<Function(const typename Container::value_type&)>::type> ret;
       ret.reserve(cont.size());
       for (const auto &v : cont) {
          ret.push_back(fun(v));
       }
       return ret;
   }

   class blacklist_plugin_impl {
      public:

         account_name producer_name;
         fc::crypto::private_key _blacklist_private_key;
         chain::public_key_type _blacklist_public_key;
         std::string actor_blacklist_hash = "";

         void check_blacklist() {
            ilog("blacklist hash: ${hash}", ("hash", actor_blacklist_hash));
         }

         std::string generate_hash(std::vector<std::string> &actors)
          {
            sort(actors.begin(), actors.end());
            auto output=apply(actors,[](std::string element){
              std::ostringstream stringStream;
              stringStream << "actor-blacklist=" << element << "\n";
              return stringStream.str();
            });
            std::string actor_str = std::accumulate(output.begin(), output.end(), std::string(""));
            ilog("actors in generate_hash: ${a}", ("a", actors));
            return (std::string)fc::sha256::hash(actor_str);
          }

         std::vector<std::string> get_local_actor_blacklist()
         {
            chain::controller& chain = app().get_plugin<chain_plugin>().chain();
            auto actor_blacklist = chain.get_actor_blacklist();
            auto accounts=apply(actor_blacklist,[](account_name element){
              std::ostringstream stringStream;
              stringStream << element.to_string();
              return stringStream.str();
            });
            return accounts;
         }

         std::string get_submitted_hash()
         {
            auto ro_api = app().get_plugin<chain_plugin>().get_read_only_api();
            eosio::chain_apis::read_only::get_table_rows_params p;
        
            p.code = eosio::chain::string_to_name("theblacklist");
            p.scope = "theblacklist";
            p.table = eosio::chain::string_to_name("producerhash");
            p.limit = 100; // TODO, will became a BUG if rows are more than 100
            p.json = true;
            std::vector<std::string> actors;
            std::string hash="";
            auto rows = ro_api.get_table_rows(p).rows;
            ilog("producerhash rows: ${a}\n", ("a", rows));
            for ( auto &row : rows ) {
               ilog("producerhash row: ${a}\n", ("a", row));
               auto obj = row.get_object();
               ilog("producerhash row hash: ${a}\n", ("a", obj["hash"]));
               ilog("producerhash row producer: ${a}\n", ("a", obj["producer"]));
               if (obj["producer"] == producer_name) {
                  hash = obj["hash"];
                  break;
               }
            }
            return hash;
         }

         std::vector<std::string> get_onchain_actor_blacklist()
         {
            auto ro_api = app().get_plugin<chain_plugin>().get_read_only_api();
            eosio::chain_apis::read_only::get_table_rows_params p;
            
            p.code = eosio::chain::string_to_name("theblacklist");
            p.scope = "theblacklist";
            p.table = eosio::chain::string_to_name("theblacklist");
            p.limit = 100; // TODO, will became a BUG if rows are more than 100
            p.json = true;
            std::vector<std::string> accounts;
            auto rows = ro_api.get_table_rows(p).rows;
            //rows is a vector<fc::variant> type
            for ( auto &row : rows ) {
              if (row["type"] == "actor-blacklist") {
                 for ( auto &account : row["accounts"].get_array() ) {
                    ilog("account: ${a}\n", ("a", account));
                    accounts.push_back(account.as_string());
                 }
              }
            }
            return accounts;
         }


  
   };

   blacklist_plugin::blacklist_plugin():my(new blacklist_plugin_impl()){}
   blacklist_plugin::~blacklist_plugin(){}

   blacklist_stats blacklist_plugin::check_hash() {
      auto local_blacklist_accounts = my->get_local_actor_blacklist();
      auto onchain_blacklist_accounts = my->get_onchain_actor_blacklist();
      ilog("local actors: ${a}\n", ("a", local_blacklist_accounts));
      ilog("on chain actors: ${a}\n", ("a", onchain_blacklist_accounts));

      blacklist_stats ret;
      ret.local_hash = my->generate_hash(local_blacklist_accounts);
      ret.ecaf_hash = my->generate_hash(onchain_blacklist_accounts);
      ret.submitted_hash = my->get_submitted_hash();
      ret.msg = "";
      if(ret.local_hash != ret.ecaf_hash) {
         ret.msg += "local and ecaf hash MISMATCH!";
      } else if(ret.local_hash != ret.submitted_hash) {
         ret.msg += "local and submitted hash MISMATCH!";
      } else {
         ret.msg += "OK";
      }
      return ret;
   }

   void blacklist_plugin::set_program_options(options_description&, options_description& cfg) {

      cfg.add_options()
            ("blacklist-signature-provider", bpo::value<string>()->default_value("HEARTBEAT_PUB_KEY=KEY:HEARTBEAT_PRIVATE_KEY"),
             "Blacklist key provider")
            ("blacklist-contract", bpo::value<string>()->default_value("theblacklist"),
             "Blacklist Contract")
            ("blacklist-permission", bpo::value<string>()->default_value("blacklist"),
             "Blacklist permission name")    
            ;
   }

   void blacklist_plugin::plugin_initialize(const variables_map& options) {
      try {

         const auto& _http_plugin = app().get_plugin<http_plugin>();
         if( !_http_plugin.is_on_loopback()) {
            wlog( "\n"
                  "**********SECURITY WARNING**********\n"
                  "*                                  *\n"
                  "* --       Blacklist API        -- *\n"
                  "* - EXPOSED to the LOCAL NETWORK - *\n"
                  "* - USE ONLY ON SECURE NETWORKS! - *\n"
                  "*                                  *\n"
                  "************************************\n" );

         }

         if(options.count("producer-name")){
             const std::vector<std::string>& ops = options["producer-name"].as<std::vector<std::string>>();
             my->producer_name = ops[0];
         }

         if(options.count("actor-blacklist")){
             auto blacklist_actors = options["actor-blacklist"].as<std::vector<std::string>>();
             my->actor_blacklist_hash = my->generate_hash(blacklist_actors);
         }

         if( options.count("blacklist-signature-provider") ) {
               auto key_spec_pair = options["blacklist-signature-provider"].as<std::string>();
               
               try {
                  auto delim = key_spec_pair.find("=");
                  EOS_ASSERT(delim != std::string::npos, eosio::chain::plugin_config_exception, "Missing \"=\" in the key spec pair");
                  auto pub_key_str = key_spec_pair.substr(0, delim);
                  auto spec_str = key_spec_pair.substr(delim + 1);
      
                  auto spec_delim = spec_str.find(":");
                  EOS_ASSERT(spec_delim != std::string::npos, eosio::chain::plugin_config_exception, "Missing \":\" in the key spec pair");
                  auto spec_type_str = spec_str.substr(0, spec_delim);
                  auto spec_data = spec_str.substr(spec_delim + 1);
      
                  auto pubkey = public_key_type(pub_key_str);
                  
                  
                  if (spec_type_str == "KEY") {
                     ilog("blacklist key loaded");
                     my->_blacklist_private_key = fc::crypto::private_key(spec_data);
                     my->_blacklist_public_key = pubkey;
                  } else if (spec_type_str == "KEOSD") {
                     elog("KEOSD blacklist key not supported");
                     // not supported
                  }
      
               } catch (...) {
                  elog("invalid blacklist signature provider: \"${val}\", ignoring!", ("val", key_spec_pair));
               }
         }
      }
      FC_LOG_AND_RETHROW()
   }

   void blacklist_plugin::plugin_startup() {
     ilog("starting blacklist_plugin");
      app().get_plugin<http_plugin>().add_api({
          CALL(blacklist, this, check_hash,
               INVOKE_R_V(this, check_hash), 200),
      });
     try{
        my->check_blacklist();
     }
     FC_LOG_AND_DROP();
   }

   void blacklist_plugin::plugin_shutdown() {
      // OK, that's enough magic
   }

}
