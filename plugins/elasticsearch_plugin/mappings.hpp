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
                        },
                        "block": {
                            "properties": {
                                "transactions": {
                                    "enabled": false
                                }
                            }
                        }
                    }
                },
                "createAt": {
                    "type": "date"
                }
            }
        },
        "accounts": {
            "properties": {
                "createAt": {
                    "type": "date"
                },
                "abi": {
                    "enabled": false
                }
            }
        }
    }
}

)";

}
