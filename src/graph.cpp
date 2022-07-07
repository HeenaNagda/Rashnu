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

#include <unordered_map>
#include "hotstuff/graph.h"
#include "hotstuff/entity.h"

#define LOG_INFO HOTSTUFF_LOG_INFO
#define LOG_DEBUG HOTSTUFF_LOG_DEBUG
#define LOG_WARN HOTSTUFF_LOG_WARN
#define LOG_PROTO HOTSTUFF_LOG_PROTO

namespace hotstuff {
    CondensationGraph::CondensationGraph(std::unordered_map<uint256_t, std::unordered_set<uint256_t>> const& graph){
        CondensationGraph::graph = graph;
        reset_visited();
        create_transposed_graph();
    }

    void CondensationGraph::reset_visited(){
        for(auto const &entry: graph) {
            visited[entry.first] = false;
        }
    }

    void CondensationGraph::create_transposed_graph(){
        for(auto const &entry: graph){
            uint256_t vertex = entry.first;
            /* We need all the vertices preset as a key in transposed graph even if value is an empty set */
            transposed_graph.insert(std::make_pair(vertex, std::unordered_set<uint256_t>()));
            /* Explore all the childs of the vertex of original graph */
            for(auto const &child_vertex: entry.second){
                transposed_graph[child_vertex].insert(vertex);
            }
        }
    }

    std::unordered_set<uint256_t> CondensationGraph::get_decendents_from_transposed_graph(uint256_t tx) {
        return transposed_graph[tx];
    }

    std::vector<std::vector<uint256_t>> CondensationGraph::get_condensation_graph() {
        /* Order vertices by their finish time */
        for(auto const &entry: graph) {
            uint256_t vertex = entry.first;
            if(visited[vertex]){
                continue;
            }
            create_order_by_finish_time(vertex);
        }

        /* reset visited as we are re-using it */
        reset_visited();

        /* Generate SCC in topological order */
        while(!order_by_finish_time.empty()) {
            uint256_t vertex = order_by_finish_time.top();
            order_by_finish_time.pop();

            if(visited[vertex]){
                continue;
            }
            std::vector<uint256_t> scc;
            generate_scc(vertex, scc);
            condensation_graph.push_back(scc);
        }

        /* Topologically sorted condensation graph */
        return condensation_graph;
    }

    void CondensationGraph::create_order_by_finish_time(uint256_t vertex) {
        visited[vertex] = true;

        /* explore children of this vertex */
        for(auto const child_vertex: graph[vertex]) {
            if(visited[child_vertex]){
                continue;
            }
            create_order_by_finish_time(child_vertex);
        } 
        order_by_finish_time.push(vertex);
    }

    void CondensationGraph::generate_scc(uint256_t vertex, std::vector<uint256_t> &scc){
        visited[vertex] = true;
        scc.push_back(vertex);

        /* explore children of this vertex */
        for(auto const child_vertex: transposed_graph[vertex]) {
            if(visited[child_vertex]){
                continue;
            }
            generate_scc(child_vertex, scc);
        } 
    }
}
