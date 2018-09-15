file(GLOB HEADERS "include/eosio/blacklist_plugin/*.hpp")
add_library( blacklist_plugin
             blacklist_plugin.cpp
             ${HEADERS} )

target_link_libraries( blacklist_plugin chain_plugin producer_plugin appbase fc )
target_include_directories( blacklist_plugin PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" )
