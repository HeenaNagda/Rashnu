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

    LocalOrderDAG::LocalOrderDAG(const std::vector<uint256_t> &order){
        this->order = order;
    }

    void LocalOrderDAG::add_dependency(uint256_t cmd_hash, std::unordered_map<uint64_t, char> dependency_info){
        for(auto info: dependency_info){
            this->dependencies[cmd_hash].insert(info.first);
            this->dependency_types[cmd_hash][info.first] = info.second;
        }
    }

    std::unordered_map<uint256_t, std::unordered_set<uint256_t>> LocalOrderDAG::create_dag(){
        /** Create an empty graph G=(V,E) **/
        std::unordered_map<uint256_t, std::unordered_set<uint256_t>> dag;

        /** For v = 1 -> m: V = V U v **/
        for(auto cmd: order){
            dag[cmd] = std::unordered_set<uint256_t>();
        }

        /** Update edges based on dependency **/
        size_t n = order.size();
        for(int curr=1; curr<n; curr++){
            auto curr_tx = order[curr];

            /** Check the dependency between T[i] and T[j] **/
            for(auto dependency: dependencies[curr_tx]){
 
                for(int prev=curr-1; prev>=0; prev--){
                    auto prev_tx = order[prev];
                    HOTSTUFF_LOG_DEBUG("curr_tx [%ld] %.10s, prev_tx [%ld] %.10s", curr, get_hex(curr_tx).c_str(), prev, get_hex(prev_tx).c_str());
                    HOTSTUFF_LOG_DEBUG("dependency = %ld, curr type = %c, dependencies[prev_tx].count(dependency) = %d", dependency, dependency_types[curr_tx][dependency], dependencies[prev_tx].count(dependency));
                
                    if(dependencies[prev_tx].count(dependency)>0 
                        && (dependency_types[curr_tx][dependency]=='w'
                        ||(dependency_types[curr_tx][dependency]=='r'
                        && dependency_types[prev_tx][dependency]=='w'))){
                            /** dependency found **/
                            /** Add an edge prev->curr **/

                            dag[prev_tx].insert(curr_tx);
                            HOTSTUFF_LOG_DEBUG("added edge %.10s -> %.10s", get_hex(prev_tx).c_str(), get_hex(curr_tx).c_str());
                    }
                }
            }
        }

        return dag;
    }

    TransitiveReduction::TransitiveReduction(std::unordered_map<uint256_t, std::unordered_set<uint256_t>> graph){
        this->reduced_graph = graph;
    }

    std::unordered_map<uint256_t, std::unordered_set<uint256_t>> TransitiveReduction::reduce(){
        // u -> v ----> w
        // |            ^
        // ------------>^     
        for(auto g: reduced_graph){
            auto u = g.first;
            for(auto v: g.second){
                std::unordered_set<uint256_t> visited;
                visited.insert(v);
                reduce_by_dfs(u, v, visited);
            }
        }
        return reduced_graph;
    }

    void TransitiveReduction::reduce_by_dfs(uint256_t u, uint256_t intermediate_node, std::unordered_set<uint256_t>& visited){
        for(uint256_t w: reduced_graph[intermediate_node]){
            if(visited.count(w)==0){
                if(reduced_graph[u].count(w)>0){
                    /** redundant edge found : Remove from the graph**/
                    reduced_graph[u].erase(w);
                }
                visited.insert(w);
                reduce_by_dfs(u, w, visited);
            }
        }
    }

    WeaklyConnectedGraph::WeaklyConnectedGraph(std::unordered_map<uint256_t, std::unordered_set<uint256_t>>& graph) {
        this->graph = graph;
    }

    std::vector<std::vector<uint256_t>> WeaklyConnectedGraph::get_wcc() {
        create_undirected_graph();

        /** Traverse undirected graph and find the connected components **/
        std::vector<std::vector<uint256_t>> wcc;
        for(auto g: undirected_graph){
            auto node = g.first;
            if(visited.count(node)==0){
                visited.insert(node);
                std::vector<uint256_t> component;
                dfs(node, component);
                wcc.push_back(component);
            }
        }

        return wcc;
    }

    void WeaklyConnectedGraph::create_undirected_graph(){
        for(auto g: graph){
            undirected_graph[g.first] = std::unordered_set<uint256_t>();
        }

        for(auto g: graph){
            auto from = g.first;
            for(auto to: g.second){
                undirected_graph[from].insert(to);
                undirected_graph[to].insert(from);
            }
        }
    }

    void WeaklyConnectedGraph::dfs(uint256_t node, std::vector<uint256_t>& component){
        component.push_back(node);
        for(auto child: undirected_graph[node]){
            if(visited.count(child)==0){
                visited.insert(child);
                dfs(child, component);
            }
        }
    }
}
