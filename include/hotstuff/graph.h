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
#include "salticidae/ref.h"

#define MAX_PROPOSAL_SIZE_SUPPORTED 960

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

class LocalOrderDAG {
    std::vector<uint256_t> order;
    // Map<cmd_hash, Pair(dependency set, Map<dependency,dependency type>)>
    std::unordered_map<uint256_t, std::unordered_set<uint64_t>> dependencies;
    std::unordered_map<uint256_t, std::unordered_map<uint64_t, char>> dependency_types;

    public:
    LocalOrderDAG(const std::vector<uint256_t> &order);
    void add_dependency(uint256_t cmd_hash, std::unordered_map<uint64_t, char> dependency_info);
    std::unordered_map<uint256_t, std::unordered_set<uint256_t>> create_dag();
};

class TransitiveReduction {
    std::unordered_map<uint256_t, std::unordered_set<uint256_t>> reduced_graph;
    void reduce_by_dfs(uint256_t u, uint256_t intermediate_node, std::unordered_set<uint256_t>& visited);

    public:
    TransitiveReduction(std::unordered_map<uint256_t, std::unordered_set<uint256_t>> graph);
    std::unordered_map<uint256_t, std::unordered_set<uint256_t>> reduce();
};

class WeaklyConnectedGraph{
    std::unordered_map<uint256_t, std::unordered_set<uint256_t>> graph;
    std::unordered_map<uint256_t, std::unordered_set<uint256_t>> undirected_graph;
    std::unordered_set<uint256_t> visited;

    void create_undirected_graph();
    void dfs(uint256_t node, std::vector<uint256_t>& component);

    public:
    WeaklyConnectedGraph(std::unordered_map<uint256_t, std::unordered_set<uint256_t>>& graph);
    std::vector<std::vector<uint256_t>> get_wcc();
};

class GraphFormatConversion{
    public:
    GraphFormatConversion();
    void serialize(DataStream &s, const std::unordered_map<uint256_t, std::unordered_set<uint256_t>> &graph);
    void unserialize(DataStream &s, std::unordered_map<uint256_t, std::unordered_set<uint256_t>> &graph);

};

class GraphOperation{
private:
    std::unordered_map<uint256_t, std::unordered_set<uint256_t>> graph;
    std::unordered_set<uint256_t> roots;
    std::unordered_map<uint256_t, size_t> n_incoming;
    size_t leaf_count;
public:
    GraphOperation(std::unordered_map<uint256_t, std::unordered_set<uint256_t>> graph);
    std::unordered_set<uint256_t> get_roots();
    std::unordered_map<salticidae::uint256_t, size_t> get_incoming_count();
    size_t get_leaf_count();
};

}
#endif

