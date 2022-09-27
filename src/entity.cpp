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
 */

#include "hotstuff/entity.h"
#include "hotstuff/hotstuff.h"

namespace hotstuff {

// Themis
// void Block::serialize(DataStream &s) const {
//     s << htole((uint32_t)parent_hashes.size());
//     for (const auto &hash: parent_hashes)
//         s << hash;
//     s << htole((uint32_t)cmds.size());
//     for (auto cmd: cmds)
//         s << cmd;
//     s << *qc << htole((uint32_t)extra.size()) << extra;
// }

// Themis
void Block::serialize(DataStream &s) const {
    /** Serialize parent hashes **/
    s << htole((uint32_t)parent_hashes.size());
    for (const auto &hash: parent_hashes){
        s << hash;
    }
    
    /** Serialize graph **/
    HOTSTUFF_LOG_DEBUG("[[serialize]] G size = %ld", graph.size());
    s << htole((uint32_t)graph.size());

    /* store all the graph nodes into a vector */
    std::vector<uint256_t> keys;
    for (auto g: graph){
        keys.push_back(g.first);
    }

    /* Sort all the nodes of graph */
    std::sort(keys.begin(), keys.end(), 
        [](const uint256_t & a, const uint256_t & b) { return a.operator<(b);});

    /* Create a node_to_idx hash map */
    std::unordered_map<uint256_t, size_t> node_to_idx;
    for(size_t idx=0; idx<graph.size(); idx++){
        node_to_idx[keys[idx]] = idx;
    }

    /* Optimize graph serialization */
    for (auto const &key: keys){
        HOTSTUFF_LOG_DEBUG("[[serialize]] added key = %.10s", key.to_hex().c_str());
        s << key;

        std::vector<uint64_t> vals;
        for(int i=0; i<MAX_PROPOSAL_SIZE_SUPPORTED/64; i++){
            vals.push_back(0x00);
        }
        for(auto const &neighbor: graph.at(key)){
            HOTSTUFF_LOG_DEBUG("[[serialize]] neighbor is = %.10s", neighbor.to_hex().c_str());
            int group_i = node_to_idx[neighbor]/64;
            vals[group_i] = vals[group_i] | (1UL << (node_to_idx[neighbor]-(64*group_i)));
            HOTSTUFF_LOG_DEBUG("[[serialize]] group number = %d, group = 0x%x, bit set = %d", group_i, vals[group_i], node_to_idx[neighbor]-(64*group_i));
        }

        /* IMPORTANT: Max block size supported is 64*8*4=2176 */
        for(auto val: vals){
            HOTSTUFF_LOG_DEBUG("[[serialize]] added group = 0x%x (%lu)", val, val);
            s << htole(val);
        }
    }

    /** Serialize e_update **/
    s << htole((uint32_t)e_update.size());
    for (auto edge: e_update){
        s << edge.first;
        s << edge.second;
    }

    /** Serialize QC **/
    s << *qc << htole((uint32_t)extra.size()) << extra;
}

// Themis
// void Block::unserialize(DataStream &s, HotStuffCore *hsc) {
//     uint32_t n;
//     s >> n;
//     n = letoh(n);
//     parent_hashes.resize(n);
//     for (auto &hash: parent_hashes)
//         s >> hash;
//     s >> n;
//     n = letoh(n);
//     cmds.resize(n);
//     for (auto &cmd: cmds)
//         s >> cmd;
// //    for (auto &cmd: cmds)
// //        cmd = hsc->parse_cmd(s);
//     qc = hsc->parse_quorum_cert(s);
//     s >> n;
//     n = letoh(n);
//     if (n == 0)
//         extra.clear();
//     else
//     {
//         auto base = s.get_data_inplace(n);
//         extra = bytearray_t(base, base + n);
//     }
//     this->hash = salticidae::get_hash(*this);
// }

// Themis
void Block::unserialize(DataStream &s, HotStuffCore *hsc) {
    uint32_t n;

    /** unserialize parent hashes **/
    s >> n;
    n = letoh(n);
    parent_hashes.resize(n);
    for (auto &hash: parent_hashes){
        s >> hash;
    }
        

    /** unserialize graph **/
    s >> n;
    n = letoh(n);
    HOTSTUFF_LOG_DEBUG("[[unserialize]] G size = %ld", n);

    std::vector<std::pair<uint256_t, std::vector<uint64_t>>> adj_matrix;
    uint256_t key;
    uint64_t val;
    for(size_t key_i=0; key_i<n; key_i++){
        s >> key;
        HOTSTUFF_LOG_DEBUG("[[unserialize]] extracted key = %.10s", key.to_hex().c_str());
        std::vector<uint64_t> vals;

        for(int i=0; i<MAX_PROPOSAL_SIZE_SUPPORTED/64; i++){
            s >> val;
            val = letoh(val);
            HOTSTUFF_LOG_DEBUG("[[unserialize]] extracted group = %.ld", val);
            vals.push_back(val);
        }

        adj_matrix.push_back(std::make_pair(key, vals));
    }
    /* Create a graph using adj matrix */
    HOTSTUFF_LOG_DEBUG("[[unserialize]] number of nodes = %ld", adj_matrix.size());
    for(auto const &element: adj_matrix){
        graph[element.first] = std::unordered_set<uint256_t>();
        HOTSTUFF_LOG_DEBUG("[[unserialize]] adj matrix key picked = %.10s", element.first.to_hex().c_str());
        for(int group_i=0; group_i<MAX_PROPOSAL_SIZE_SUPPORTED/64; group_i++){
            uint64_t group = element.second[group_i];
            HOTSTUFF_LOG_DEBUG("[[unserialize]] group number = %d, group = 0x%x", group_i, group);
            if(group==0){
                continue;
            }
            for(int i=0; i<64; i++){
                if(((uint64_t)group & (1UL<<i)) != 0){
                    HOTSTUFF_LOG_DEBUG("[[unserialize]] Matrix index = %d, anding = 0x%x", (i+(group_i*64)), group&(1<<i));
                    graph[element.first].insert(adj_matrix[i+(group_i*64)].first);
                    HOTSTUFF_LOG_DEBUG("[[unserialize]] Created graph = %.10s -> %.10s", element.first.to_hex().c_str(), adj_matrix[i+(group_i*64)].first.to_hex().c_str());
                }
            }
        }
    }

    /** unserialize e_update **/
    s >> n;
    n = letoh(n);
    e_update.resize(n);
    for (auto &edge: e_update) {
        s >> edge.first;
        s >> edge.second;
    }

    /** unserialize QC **/
    qc = hsc->parse_quorum_cert(s);
    s >> n;
    n = letoh(n);
    if (n == 0)
        extra.clear();
    else
    {
        auto base = s.get_data_inplace(n);
        extra = bytearray_t(base, base + n);
    }
    this->hash = salticidae::get_hash(*this);
}

bool Block::verify(const HotStuffCore *hsc) const {
    if (qc->get_obj_hash() == hsc->get_genesis()->get_hash())
        return true;
    return qc->verify(hsc->get_config());
}

promise_t Block::verify(const HotStuffCore *hsc, VeriPool &vpool) const {
    if (qc->get_obj_hash() == hsc->get_genesis()->get_hash())
        return promise_t([](promise_t &pm) { pm.resolve(true); });
    return qc->verify(hsc->get_config(), vpool);
}

}
