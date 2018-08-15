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
using chain::block_trace;
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

   void init();
   void delete_index();

   bool configured{false};
   bool delete_index_on_startup{false};
   uint32_t start_block_num = 0;
   bool start_block_reached = false;

   std::shared_ptr<elasticsearch_helper> elastic_helper;

   size_t queue_size = 0;
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

namespace {

template<typename Queue, typename Entry>
void queue(boost::mutex& mtx, boost::condition_variable& condition, Queue& queue, const Entry& e, size_t queue_size) {
   int sleep_time = 100;
   size_t last_queue_size = 0;
   boost::mutex::scoped_lock lock(mtx);
   if (queue.size() > queue_size) {
      lock.unlock();
      condition.notify_one();
      if (last_queue_size < queue.size()) {
         sleep_time += 100;
      } else {
         sleep_time -= 100;
         if (sleep_time < 0) sleep_time = 100;
      }
      last_queue_size = queue.size();
      boost::this_thread::sleep_for(boost::chrono::milliseconds(sleep_time));
      lock.lock();
   }
   queue.emplace_back(e);
   lock.unlock();
   condition.notify_one();
}

}

void elasticsearch_plugin_impl::accepted_transaction( const chain::transaction_metadata_ptr& t ) {
   try {
      queue( mtx, condition, transaction_metadata_queue, t, queue_size );
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
      queue( mtx, condition, transaction_trace_queue, t, queue_size );
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
      queue( mtx, condition, irreversible_block_state_queue, bs, queue_size );
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
      queue( mtx, condition, block_state_queue, bs, queue_size );
   } catch (fc::exception& e) {
      elog("FC Exception while accepted_block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while accepted_block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while accepted_block");
   }
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

   fc::mutable_variant_object block_states_doc;
   block_states_doc["block_num"] = static_cast<int32_t>(block_num);
   block_states_doc["block_id"] = block_id_str;
   block_states_doc["validated"] = bs->validated;
   block_states_doc["in_current_chain"] = bs->in_current_chain;
   block_states_doc["block_header_state"] = bs;
   block_states_doc["createAt"] = now.count();

   auto block_states_json = fc::json::to_pretty_string( block_states_doc );

   std::cout << block_states_json << std::endl;

   elastic_helper->update(block_states_type, block_states_json);


   fc::mutable_variant_object block_doc;

   block_doc["block_num"] = static_cast<int32_t>(block_num);
   block_doc["block_id"] = block_id_str;
   block_doc["irreversible"] = false;

   // TODO: to_variant_with_abi
   // block_doc["block"] = bs->block;
   block_doc["createAt"] = now.count();

   auto block_json = fc::json::to_pretty_string( block_doc );

   std::cout << block_json << std::endl;

   elastic_helper->update(blocks_type, block_json);


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

         // warn if queue size greater than 75%
         if( transaction_metadata_size > (queue_size * 0.75) ||
             transaction_trace_size > (queue_size * 0.75) ||
             block_state_size > (queue_size * 0.75) ||
             irreversible_block_size > (queue_size * 0.75)) {
            wlog("queue size: ${q}", ("q", transaction_metadata_size + transaction_trace_size + block_state_size + irreversible_block_size));
         } else if (done) {
            ilog("draining queue, size: ${q}", ("q", transaction_metadata_size + transaction_trace_size + block_state_size + irreversible_block_size));
         }

         // process transactions
         while (!transaction_metadata_process_queue.empty()) {
            const auto& t = transaction_metadata_process_queue.front();
            process_accepted_transaction(t);
            transaction_metadata_process_queue.pop_front();
         }

         while (!transaction_trace_process_queue.empty()) {
            const auto& t = transaction_trace_process_queue.front();
            process_applied_transaction(t);
            transaction_trace_process_queue.pop_front();
         }

         // process blocks
         while (!block_state_process_queue.empty()) {
            const auto& bs = block_state_process_queue.front();
            process_accepted_block( bs );
            block_state_process_queue.pop_front();
         }

         // process irreversible blocks
         while (!irreversible_block_state_process_queue.empty()) {
            const auto& bs = irreversible_block_state_process_queue.front();
            process_irreversible_block(bs);
            irreversible_block_state_process_queue.pop_front();
         }

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
   elastic_helper->init_index(elastic_mappings);

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

      my->queue_size = 256;

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
