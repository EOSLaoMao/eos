/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
#include <eosio/elasticsearch_plugin/elasticsearch_plugin.hpp>
#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>
#include <fc/variant_object.hpp>

#include <boost/chrono.hpp>
#include <boost/format.hpp>
#include <boost/signals2/connection.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#include <queue>


#include "elasticsearch_helper.hpp"
#include "mappings.hpp"


namespace eosio {

using chain::account_name;
using chain::action_name;
using chain::block_id_type;
using chain::permission_name;
using chain::transaction;
using chain::signed_transaction;
using chain::signed_block;
using chain::transaction_id_type;
using chain::packed_transaction;

static appbase::abstract_plugin& _elasticsearch_plugin = app().register_plugin<elasticsearch_plugin>();

class elasticsearch_plugin_impl {
public:
   elasticsearch_plugin_impl();
   ~elasticsearch_plugin_impl();

   fc::optional<boost::signals2::scoped_connection> accepted_block_connection;
   fc::optional<boost::signals2::scoped_connection> irreversible_block_connection;
   fc::optional<boost::signals2::scoped_connection> accepted_transaction_connection;
   fc::optional<boost::signals2::scoped_connection> applied_transaction_connection;

   void consume_blocks();

   void accepted_block( const chain::block_state_ptr& );
   void applied_irreversible_block(const chain::block_state_ptr&);
   void accepted_transaction(const chain::transaction_metadata_ptr&);
   void applied_transaction(const chain::transaction_trace_ptr&);
   void process_accepted_transaction(const chain::transaction_metadata_ptr&);
   void _process_accepted_transaction(const chain::transaction_metadata_ptr&);
   void process_applied_transaction(const chain::transaction_trace_ptr&);
   void _process_applied_transaction(const chain::transaction_trace_ptr&);
   void process_accepted_block( const chain::block_state_ptr& );
   void _process_accepted_block( const chain::block_state_ptr& );
   void process_irreversible_block(const chain::block_state_ptr&);
   void _process_irreversible_block(const chain::block_state_ptr&);

   optional<abi_serializer> get_abi_serializer( account_name n );
   template<typename T> fc::variant to_variant_with_abi( const T& obj );
   bool search_abi_by_account(fc::variant &v, const std::string &name);
   void purge_abi_cache();

   void init();
   void delete_index();

   template<typename Queue, typename Entry> void queue(Queue& queue, const Entry& e);

   bool configured{false};
   bool delete_index_on_startup{false};
   uint32_t start_block_num = 0;
   bool start_block_reached = false;

   std::shared_ptr<elasticsearch_helper> elastic_helper;

   size_t max_queue_size = 0;
   int queue_sleep_time = 0;
   size_t abi_cache_size = 0;
   std::deque<chain::transaction_metadata_ptr> transaction_metadata_queue;
   std::deque<chain::transaction_metadata_ptr> transaction_metadata_process_queue;
   std::deque<chain::transaction_trace_ptr> transaction_trace_queue;
   std::deque<chain::transaction_trace_ptr> transaction_trace_process_queue;
   std::deque<chain::block_state_ptr> block_state_queue;
   std::deque<chain::block_state_ptr> block_state_process_queue;
   std::deque<chain::block_state_ptr> irreversible_block_state_queue;
   std::deque<chain::block_state_ptr> irreversible_block_state_process_queue;
   boost::mutex mtx;
   boost::condition_variable condition;
   boost::thread consume_thread;
   boost::atomic<bool> done{false};
   boost::atomic<bool> startup{true};
   fc::optional<chain::chain_id_type> chain_id;
   fc::microseconds abi_serializer_max_time;

   struct by_account;
   struct by_last_access;

   struct abi_cache {
      account_name                     account;
      fc::time_point                   last_accessed;
      fc::optional<abi_serializer>     serializer;
   };

   typedef boost::multi_index_container<abi_cache,
         indexed_by<
               ordered_unique< tag<by_account>,  member<abi_cache,account_name,&abi_cache::account> >,
               ordered_non_unique< tag<by_last_access>,  member<abi_cache,fc::time_point,&abi_cache::last_accessed> >
         >
   > abi_cache_index_t;

   abi_cache_index_t abi_cache_index;

   static const account_name newaccount;
   static const account_name setabi;

   static const std::string block_states_type;
   static const std::string blocks_type;
   static const std::string trans_type;
   static const std::string trans_traces_type;
   static const std::string actions_type;
   static const std::string accounts_type;
};

const account_name elasticsearch_plugin_impl::newaccount = "newaccount";
const account_name elasticsearch_plugin_impl::setabi = "setabi";

const std::string elasticsearch_plugin_impl::block_states_type = "block_states";
const std::string elasticsearch_plugin_impl::blocks_type = "blocks";
const std::string elasticsearch_plugin_impl::trans_type = "transactions";
const std::string elasticsearch_plugin_impl::trans_traces_type = "transaction_traces";
const std::string elasticsearch_plugin_impl::actions_type = "actions";
const std::string elasticsearch_plugin_impl::accounts_type = "accounts";


elasticsearch_plugin_impl::elasticsearch_plugin_impl()
{
}

elasticsearch_plugin_impl::~elasticsearch_plugin_impl()
{
   if (!startup) {
      try {
         ilog( "elasticsearch_plugin shutdown in process please be patient this can take a few minutes" );
         done = true;
         condition.notify_one();

         consume_thread.join();
      } catch( std::exception& e ) {
         elog( "Exception on elasticsearch_plugin shutdown of consume thread: ${e}", ("e", e.what()));
      }
   }
}

template<typename Queue, typename Entry>
void elasticsearch_plugin_impl::queue( Queue& queue, const Entry& e ) {
   boost::mutex::scoped_lock lock( mtx );
   auto queue_size = queue.size();
   if( queue_size > max_queue_size ) {
      lock.unlock();
      condition.notify_one();
      queue_sleep_time += 10;
      if( queue_sleep_time > 1000 )
         wlog("queue size: ${q}", ("q", queue_size));
      boost::this_thread::sleep_for( boost::chrono::milliseconds( queue_sleep_time ));
      lock.lock();
   } else {
      queue_sleep_time -= 10;
      if( queue_sleep_time < 0 ) queue_sleep_time = 0;
   }
   queue.emplace_back( e );
   lock.unlock();
   condition.notify_one();
}

void elasticsearch_plugin_impl::accepted_transaction( const chain::transaction_metadata_ptr& t ) {
   try {
      queue( transaction_metadata_queue, t );
   } catch (fc::exception& e) {
      elog("FC Exception while accepted_transaction ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while accepted_transaction ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while accepted_transaction");
   }
}

void elasticsearch_plugin_impl::applied_transaction( const chain::transaction_trace_ptr& t ) {
   try {
      queue( transaction_trace_queue, t );
   } catch (fc::exception& e) {
      elog("FC Exception while applied_transaction ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while applied_transaction ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while applied_transaction");
   }
}

void elasticsearch_plugin_impl::applied_irreversible_block( const chain::block_state_ptr& bs ) {
   try {
      queue( irreversible_block_state_queue, bs );
   } catch (fc::exception& e) {
      elog("FC Exception while applied_irreversible_block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while applied_irreversible_block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while applied_irreversible_block");
   }
}

void elasticsearch_plugin_impl::accepted_block( const chain::block_state_ptr& bs ) {
   try {
      queue( block_state_queue, bs );
   } catch (fc::exception& e) {
      elog("FC Exception while accepted_block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while accepted_block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while accepted_block");
   }
}

void elasticsearch_plugin_impl::purge_abi_cache() {
   if( abi_cache_index.size() < abi_cache_size ) return;

   // remove the oldest (smallest) last accessed
   auto& idx = abi_cache_index.get<by_last_access>();
   auto itr = idx.begin();
   if( itr != idx.end() ) {
      idx.erase( itr );
   }
}

bool elasticsearch_plugin_impl::search_abi_by_account(fc::variant &v, const std::string &name) {
   fc::variant res;
   std::string query = boost::str(boost::format(R"({"query" : { "term" : { "name" : "%1%" }}})") % name);
   elastic_helper->search(res, accounts_type, query);
   if(res["hits"]["total"] != 1) return false;

   size_t pos = 0;
   try {
      v = res["hits"]["hits"][pos]["_source"]["abi"];
   } catch( ... ) {
      return false;
   }

   return true;
}

optional<abi_serializer> elasticsearch_plugin_impl::get_abi_serializer( account_name n ) {
   if( n.good()) {
      try {

         auto itr = abi_cache_index.find( n );
         if( itr != abi_cache_index.end() ) {
            abi_cache_index.modify( itr, []( auto& entry ) {
               entry.last_accessed = fc::time_point::now();
            });

            return itr->serializer;
         }

         fc::variant abi_v;
         if(search_abi_by_account(abi_v, n.to_string())) {
            abi_def abi;
            try {
               abi = abi_v.as<abi_def>();
            } catch (...) {
               ilog( "Unable to convert account abi to abi_def for ${n}", ( "n", n ));
               return optional<abi_serializer>();
            }

            purge_abi_cache(); // make room if necessary
            abi_cache entry;
            entry.account = n;
            entry.last_accessed = fc::time_point::now();
            abi_serializer abis;
            if( n == chain::config::system_account_name ) {
               // redefine eosio setabi.abi from bytes to abi_def
               // Done so that abi is stored as abi_def in mongo instead of as bytes
               auto itr = std::find_if( abi.structs.begin(), abi.structs.end(),
                                          []( const auto& s ) { return s.name == "setabi"; } );
               if( itr != abi.structs.end() ) {
                  auto itr2 = std::find_if( itr->fields.begin(), itr->fields.end(),
                                             []( const auto& f ) { return f.name == "abi"; } );
                  if( itr2 != itr->fields.end() ) {
                     if( itr2->type == "bytes" ) {
                        itr2->type = "abi_def";
                        // unpack setabi.abi as abi_def instead of as bytes
                        abis.add_specialized_unpack_pack( "abi_def",
                              std::make_pair<abi_serializer::unpack_function, abi_serializer::pack_function>(
                                    []( fc::datastream<const char*>& stream, bool is_array, bool is_optional ) -> fc::variant {
                                       EOS_ASSERT( !is_array && !is_optional, chain::mongo_db_exception, "unexpected abi_def");
                                       chain::bytes temp;
                                       fc::raw::unpack( stream, temp );
                                       return fc::variant( fc::raw::unpack<abi_def>( temp ) );
                                    },
                                    []( const fc::variant& var, fc::datastream<char*>& ds, bool is_array, bool is_optional ) {
                                       EOS_ASSERT( false, chain::mongo_db_exception, "never called" );
                                    }
                              ) );
                     }
                  }
               }
            }
            abis.set_abi( abi, abi_serializer_max_time );
            entry.serializer.emplace( std::move( abis ) );
            abi_cache_index.insert( entry );
            return entry.serializer;
         }
      } FC_CAPTURE_AND_LOG((n))
   }
   return optional<abi_serializer>();
}

template<typename T>
fc::variant elasticsearch_plugin_impl::to_variant_with_abi( const T& obj ) {
   fc::variant pretty_output;
   abi_serializer::to_variant( obj, pretty_output,
                               [&]( account_name n ) { return get_abi_serializer( n ); },
                               abi_serializer_max_time );
   return pretty_output;
}


void elasticsearch_plugin_impl::process_accepted_transaction( const chain::transaction_metadata_ptr& t ) {
   try {
      // always call since we need to capture setabi on accounts even if not storing transactions
      // _process_accepted_transaction(t);
   } catch (fc::exception& e) {
      elog("FC Exception while processing accepted transaction metadata: ${e}", ("e", e.to_detail_string()));
   } catch (std::exception& e) {
      elog("STD Exception while processing accepted tranasction metadata: ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while processing accepted transaction metadata");
   }
}

void elasticsearch_plugin_impl::process_applied_transaction( const chain::transaction_trace_ptr& t ) {
   try {
      if( start_block_reached ) {
         // _process_applied_transaction( t );
      }
   } catch (fc::exception& e) {
      elog("FC Exception while processing applied transaction trace: ${e}", ("e", e.to_detail_string()));
   } catch (std::exception& e) {
      elog("STD Exception while processing applied transaction trace: ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while processing applied transaction trace");
   }
}

void elasticsearch_plugin_impl::process_irreversible_block(const chain::block_state_ptr& bs) {
  try {
     if( start_block_reached ) {
      //   _process_irreversible_block( bs );
     }
  } catch (fc::exception& e) {
     elog("FC Exception while processing irreversible block: ${e}", ("e", e.to_detail_string()));
  } catch (std::exception& e) {
     elog("STD Exception while processing irreversible block: ${e}", ("e", e.what()));
  } catch (...) {
     elog("Unknown exception while processing irreversible block");
  }
}

void elasticsearch_plugin_impl::process_accepted_block( const chain::block_state_ptr& bs ) {
   try {
      if( !start_block_reached ) {
         if( bs->block_num >= start_block_num ) {
            start_block_reached = true;
         }
      }
      if( start_block_reached ) {
         _process_accepted_block( bs ); 
      }
   } catch (fc::exception& e) {
      elog("FC Exception while processing accepted block trace ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while processing accepted block trace ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while processing accepted block trace");
   }
}

void elasticsearch_plugin_impl::_process_accepted_block( const chain::block_state_ptr& bs ) {

   auto block_num = bs->block_num;
   const auto block_id = bs->id;
   const auto block_id_str = block_id.str();
   const auto prev_block_id_str = bs->block->previous.str();

   auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
         std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()});

   fc::mutable_variant_object block_state_doc;
   block_state_doc["block_num"] = static_cast<int32_t>(block_num);
   block_state_doc["block_id"] = block_id_str;
   block_state_doc["validated"] = bs->validated;
   block_state_doc["in_current_chain"] = bs->in_current_chain;
   block_state_doc["block_header_state"] = bs;
   block_state_doc["createAt"] = now.count();

   auto block_states_json = fc::json::to_string( block_state_doc );

   std::cout << block_states_json << std::endl;

   elastic_helper->index(block_states_type, block_states_json);

   fc::mutable_variant_object block_doc;

   block_doc["block_num"] = static_cast<int32_t>(block_num);
   block_doc["block_id"] = block_id_str;
   block_doc["irreversible"] = false;

   block_doc["block"] = to_variant_with_abi( *bs->block );
   block_doc["createAt"] = now.count();

   auto block_json = fc::json::to_string( block_doc );

   elastic_helper->index(blocks_type, block_json);

}

void elasticsearch_plugin_impl::consume_blocks() {
   try {
      while (true) {
         boost::mutex::scoped_lock lock(mtx);
         while ( transaction_metadata_queue.empty() &&
                 transaction_trace_queue.empty() &&
                 block_state_queue.empty() &&
                 irreversible_block_state_queue.empty() &&
                 !done ) {
            condition.wait(lock);
         }

         // capture for processing
         size_t transaction_metadata_size = transaction_metadata_queue.size();
         if (transaction_metadata_size > 0) {
            transaction_metadata_process_queue = move(transaction_metadata_queue);
            transaction_metadata_queue.clear();
         }
         size_t transaction_trace_size = transaction_trace_queue.size();
         if (transaction_trace_size > 0) {
            transaction_trace_process_queue = move(transaction_trace_queue);
            transaction_trace_queue.clear();
         }
         size_t block_state_size = block_state_queue.size();
         if (block_state_size > 0) {
            block_state_process_queue = move(block_state_queue);
            block_state_queue.clear();
         }
         size_t irreversible_block_size = irreversible_block_state_queue.size();
         if (irreversible_block_size > 0) {
            irreversible_block_state_process_queue = move(irreversible_block_state_queue);
            irreversible_block_state_queue.clear();
         }

         lock.unlock();

         if (done) {
            ilog("draining queue, size: ${q}", ("q", transaction_metadata_size + transaction_trace_size + block_state_size + irreversible_block_size));
         }

         // process transactions
         auto start_time = fc::time_point::now();
         auto size = transaction_trace_process_queue.size();
         while (!transaction_trace_process_queue.empty()) {
            const auto& t = transaction_trace_process_queue.front();
            process_applied_transaction(t);
            transaction_trace_process_queue.pop_front();
         }
         auto time = fc::time_point::now() - start_time;
         auto per = size > 0 ? time.count()/size : 0;
         if( time > fc::microseconds(500000) ) // reduce logging, .5 secs
            ilog( "process_applied_transaction,  time per: ${p}, size: ${s}, time: ${t}", ("s", size)("t", time)("p", per) );

         start_time = fc::time_point::now();
         size = transaction_metadata_process_queue.size();
         while (!transaction_metadata_process_queue.empty()) {
            const auto& t = transaction_metadata_process_queue.front();
            process_accepted_transaction(t);
            transaction_metadata_process_queue.pop_front();
         }
         time = fc::time_point::now() - start_time;
         per = size > 0 ? time.count()/size : 0;
         if( time > fc::microseconds(500000) ) // reduce logging, .5 secs
            ilog( "process_accepted_transaction, time per: ${p}, size: ${s}, time: ${t}", ("s", size)( "t", time )( "p", per ));

         // process blocks
         start_time = fc::time_point::now();
         size = block_state_process_queue.size();
         while (!block_state_process_queue.empty()) {
            const auto& bs = block_state_process_queue.front();
            process_accepted_block( bs );
            block_state_process_queue.pop_front();
         }
         time = fc::time_point::now() - start_time;
         per = size > 0 ? time.count()/size : 0;
         if( time > fc::microseconds(500000) ) // reduce logging, .5 secs
            ilog( "process_accepted_block,       time per: ${p}, size: ${s}, time: ${t}", ("s", size)("t", time)("p", per) );

         // process irreversible blocks
         start_time = fc::time_point::now();
         size = irreversible_block_state_process_queue.size();
         while (!irreversible_block_state_process_queue.empty()) {
            const auto& bs = irreversible_block_state_process_queue.front();
            process_irreversible_block(bs);
            irreversible_block_state_process_queue.pop_front();
         }
         time = fc::time_point::now() - start_time;
         per = size > 0 ? time.count()/size : 0;
         if( time > fc::microseconds(500000) ) // reduce logging, .5 secs
            ilog( "process_irreversible_block,   time per: ${p}, size: ${s}, time: ${t}", ("s", size)("t", time)("p", per) );

         if( transaction_metadata_size == 0 &&
             transaction_trace_size == 0 &&
             block_state_size == 0 &&
             irreversible_block_size == 0 &&
             done ) {
            break;
         }
      }
      ilog("elasticsearch_plugin consume thread shutdown gracefully");
   } catch (fc::exception& e) {
      elog("FC Exception while consuming block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while consuming block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while consuming block");
   }
}


void elasticsearch_plugin_impl::delete_index() {
   ilog("drop elasticsearch index");
   elastic_helper->delete_index();
}

void elasticsearch_plugin_impl::init() {
   ilog("create elasticsearch index");
   elastic_helper->init_index( elastic_mappings );

   if (elastic_helper->count_doc(accounts_type) == 0) {
      auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()});
      
      fc::mutable_variant_object account_doc;
      account_doc["name"] = name( chain::config::system_account_name ).to_string();
      account_doc["createAt"] = now.count();

      auto account_json = fc::json::to_string( account_doc );
      elastic_helper->index( accounts_type, account_json );
   }

   ilog("starting elasticsearch plugin thread");
   consume_thread = boost::thread([this] { consume_blocks(); });

   startup = false;
}

elasticsearch_plugin::elasticsearch_plugin():my(new elasticsearch_plugin_impl()){}
elasticsearch_plugin::~elasticsearch_plugin(){}

void elasticsearch_plugin::set_program_options(options_description&, options_description& cfg) {
   cfg.add_options()
         ("option-name", bpo::value<string>()->default_value("default value"),
          "Option Description")
         ;
}

void elasticsearch_plugin::plugin_initialize(const variables_map& options) {
   ilog( "initializing elasticsearch_plugin" );
   try {
      if( options.count( "option-name" )) {
         // Handle the option
      }

      my->max_queue_size = 1024;

      my->abi_cache_size = 2048;

      my->abi_serializer_max_time = app().get_plugin<chain_plugin>().get_abi_serializer_max_time();

      my->start_block_num = 0;


      if( my->start_block_num == 0 ) {
        my->start_block_reached = true;
      }

      my->delete_index_on_startup = true;

      std::string index_name = "eos";
      my->elastic_helper = std::make_shared<elasticsearch_helper>(std::vector<std::string>({"http://localhost:9200/"}), index_name);

      // hook up to signals on controller
      chain_plugin* chain_plug = app().find_plugin<chain_plugin>();
      EOS_ASSERT( chain_plug, chain::missing_chain_plugin_exception, ""  );
      auto& chain = chain_plug->chain();
      my->chain_id.emplace( chain.get_chain_id());

      my->accepted_block_connection.emplace(
         chain.accepted_block.connect( [&]( const chain::block_state_ptr& bs ) {
         my->accepted_block( bs );
      } ));
      my->irreversible_block_connection.emplace(
         chain.irreversible_block.connect( [&]( const chain::block_state_ptr& bs ) {
            my->applied_irreversible_block( bs );
         } ));
      my->accepted_transaction_connection.emplace(
         chain.accepted_transaction.connect( [&]( const chain::transaction_metadata_ptr& t ) {
            my->accepted_transaction( t );
         } ));
      my->applied_transaction_connection.emplace(
         chain.applied_transaction.connect( [&]( const chain::transaction_trace_ptr& t ) {
            my->applied_transaction( t );
         } ));
      if( my->delete_index_on_startup ) {
         my->delete_index();
      }
      my->init();
   }
   FC_LOG_AND_RETHROW()
}

void elasticsearch_plugin::plugin_startup() {
   // Make the magic happen
}

void elasticsearch_plugin::plugin_shutdown() {
   my->accepted_block_connection.reset();
   my->irreversible_block_connection.reset();
   my->accepted_transaction_connection.reset();
   my->applied_transaction_connection.reset();

   my.reset();
}

}
