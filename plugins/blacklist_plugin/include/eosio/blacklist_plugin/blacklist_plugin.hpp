/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
#pragma once
#include <appbase/application.hpp>
#include <eosio/chain_plugin/chain_plugin.hpp>
#include <eosio/producer_plugin/producer_plugin.hpp>

namespace eosio {

using namespace appbase;

/**
 *  This is a template plugin, intended to serve as a starting point for making new plugins
 */

struct blacklist_stats {
   std::string                 local_hash;
};

class blacklist_plugin : public appbase::plugin<blacklist_plugin> {
public:
   blacklist_plugin();
   virtual ~blacklist_plugin();
 
   APPBASE_PLUGIN_REQUIRES((producer_plugin)(chain_plugin))
   virtual void set_program_options(options_description&, options_description& cfg) override;
 
   void plugin_initialize(const variables_map& options);
   void plugin_startup();
   void plugin_shutdown();

private:
   std::unique_ptr<class blacklist_plugin_impl> my;
};

}
