/**
 * Copyright 2018 VMware
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 *          Author: Heena Nagda
 *           Project: Themis
 */

#ifndef _THEMIS_GRAPH_H
#define _THEMIS_GRAPH_H

#include <unordered_map>
#include "hotstuff/entity.h"

namespace hotstuff {

class CondensationGraph {
    std::vector<std::vector<uint256_t>> condensation_graph;
    std::unordered_map<uint256_t, std::unordered_set<uint256_t>> graph;
    std::unordered_map<uint256_t, std::unordered_set<uint256_t>> transposed_graph;
    std::stack<uint256_t> order_by_finish_time;
    std::unordered_map<uint256_t, bool> visited;

    void reset_visited();
    void create_transposed_graph();
    void create_order_by_finish_time(uint256_t vertex);
    void generate_scc(uint256_t vertex, std::vector<uint256_t> &);

    public:
    CondensationGraph(std::unordered_map<uint256_t, std::unordered_set<uint256_t>> const&);
    std::vector<std::vector<uint256_t>> get_condensation_graph();
    std::unordered_set<uint256_t> get_decendents_from_transposed_graph(uint256_t tx);
};

}
#endif

