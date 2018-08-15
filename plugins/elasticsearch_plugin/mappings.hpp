#include <string>

namespace eosio {

const static std::string elastic_mappings = R"(
{
    "mappings": {
        "block_states": {
            "properties": {
                "block_header_state": {
                    "properties": {
                        "producer_to_last_produced": {
                            "enabled": false
                        },
                        "producer_to_last_implied_irb": {
                            "enabled": false
                        }
                    }
                }
            }
        }
    }
}
)";

}