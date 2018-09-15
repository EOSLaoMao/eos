/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
#include <eosio/blacklist_plugin/blacklist_plugin.hpp>

namespace eosio {
   static appbase::abstract_plugin& _template_plugin = app().register_plugin<producer_blacklist_plugin>();

class producer_blacklist_plugin_impl {
   public:

      account_name producer_name;
      fc::crypto::private_key _blacklist_private_key;
      chain::public_key_type _blacklist_public_key;
      std::string actor_blacklist_hash = "";

      void check_blacklist() {
         ilog("blacklist checking");
      }
};

producer_blacklist_plugin::producer_blacklist_plugin():my(new producer_blacklist_plugin_impl()){}
producer_blacklist_plugin::~producer_blacklist_plugin(){}

void producer_blacklist_plugin::set_program_options(options_description&, options_description& cfg) {

   cfg.add_options()
         ("blacklist-signature-provider", bpo::value<string>()->default_value("HEARTBEAT_PUB_KEY=KEY:HEARTBEAT_PRIVATE_KEY"),
          "Blacklist key provider")
         ("blacklist-contract", bpo::value<string>()->default_value("theblacklist"),
          "Blacklist Contract")
         ("blacklist-permission", bpo::value<string>()->default_value("blacklist"),
          "Blacklist permission name")    
         ;
}

void producer_blacklist_plugin::plugin_initialize(const variables_map& options) {
   try {

      if(options.count("producer-name")){
          const std::vector<std::string>& ops = options["producer-name"].as<std::vector<std::string>>();
          my->producer_name = ops[0];
      }

      if(options.count("actor-blacklist")){
          auto blacklist_actors = options["actor-blacklist"].as<std::vector<std::string>>();
          sort(blacklist_actors.begin(), blacklist_actors.end());
          auto output=apply(blacklist_actors,[](std::string element){
              std::ostringstream stringStream;
              stringStream << "actor-blacklist=" << element << "\n";
              return stringStream.str();
              });
          std::string actor_blacklist_str = std::accumulate(output.begin(), output.end(), std::string(""));
          my->actor_blacklist_hash = (string)fc::sha256::hash(actor_blacklist_str);
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
               elog("Malformed signature provider: \"${val}\", ignoring!", ("val", key_spec_pair));
            }
      }
   }
   FC_LOG_AND_RETHROW()
}

void producer_blacklist_plugin::plugin_startup() {
  ilog("producer blacklist plugin:  plugin_startup() begin");
  try{
     my->check_blacklist();
  }
  FC_LOG_AND_DROP();
}

void producer_blacklist_plugin::plugin_shutdown() {
   // OK, that's enough magic
}

}
