/**
 * Copyright Soramitsu Co., Ltd. 2017 All Rights Reserved.
 * http://soramitsu.co.jp
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef IROHA_CLI_BOOTSTRAP_NETWORK_HPP
#define IROHA_CLI_BOOTSTRAP_NETWORK_HPP

#include <string>
#include <vector>
#include "genesis_block_client.hpp"
#include "model/block.hpp"

namespace iroha_cli {

  class BootstrapNetwork {
  public:
    BootstrapNetwork(GenesisBlockClient& client) : client_(client) {}

    /**
     * parse trusted peer's ip addresses in `target.conf`
     * @param target_conf_path
     * @return trusted peers' ip
     */
    std::vector<std::string> parse_trusted_peers(
        std::string const& target_conf_path);

    /**
     * parse transactions in genesis block `genesis.json`
     * @param genesis_json_path
     * @return iroha::model::Block
     */
    iroha::model::Block parse_genesis_block(
        std::string const& genesis_json_path);

    /**
     * aborts bootstrapping network.
     * @param trusted_peers
     * @param block
     */
    void abort_network(std::vector<std::string> const& trusted_peers,
                       iroha::model::Block const& block);

    /**
     * bootstraps network of trusted peers.
     * @param trusted_peers
     * @param block
     */
    void run_network(std::vector<std::string> const& trusted_peers,
                     iroha::model::Block const& block);

   private:
    iroha_cli::GenesisBlockClient& client_;
  };

}  // namespace iroha_cli
#endif  // IROHA_CLI_BOOTSTRAP_NETWORK_HPP
