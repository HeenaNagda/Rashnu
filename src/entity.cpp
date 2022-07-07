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
    // HOTSTUFF_LOG_INFO("[[Block Serialize : parent size = %d]]", parent_hashes.size());
    for (const auto &hash: parent_hashes){
        s << hash;
        // HOTSTUFF_LOG_INFO("[[Block Serialize : parent hash = %s]]", get_hex(hash).c_str());
    }
    
    /** Serialize graph **/
    s << htole((uint32_t)graph.size());
    // HOTSTUFF_LOG_INFO("[[Block Serialize : graph size = %d]]", graph.size());
    for (auto g: graph){
        s << g.first;
        // HOTSTUFF_LOG_INFO("[[Block Serialize : graph key = %s]]", get_hex(g.first).c_str());
        s << htole((uint32_t)g.second.size());
        // HOTSTUFF_LOG_INFO("[[Block Serialize : graph key-set size = %d]]", g.second.size());
        for (auto element: g.second){
            s << element;
            // HOTSTUFF_LOG_INFO("[[Block Serialize : graph set val = %s]]", get_hex(element).c_str());
        }
    }

    /** Serialize e_update **/
    s << htole((uint32_t)e_update.size());
    // HOTSTUFF_LOG_INFO("[[Block Serialize : e_update size = %d]]", e_update.size());
    for (auto edge: e_update){
        s << edge.first;
        s << edge.second;
        // HOTSTUFF_LOG_INFO("[[Block Serialize : e_update edge = %s, %s]]", get_hex(edge.first).c_str(), get_hex(edge.second).c_str());
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
    // HOTSTUFF_LOG_INFO("[[Block UnSerialize : parent size = %d]]", parent_hashes.size());
    for (auto &hash: parent_hashes){
        s >> hash;
        // HOTSTUFF_LOG_INFO("[[Block UnSerialize : parent hash = %s]]", get_hex(hash).c_str());
    }
        

    /** unserialize graph **/
    s >> n;
    n = letoh(n);
    // HOTSTUFF_LOG_INFO("[[Block UnSerialize : graph size = %d]]", n);
    uint256_t key;
    uint32_t set_size;
    uint256_t set_element;

    for(int key_i=0; key_i<n; key_i++){
        s >> key;
        graph[key] = std::unordered_set<uint256_t>();
        // HOTSTUFF_LOG_INFO("[[Block UnSerialize : graph key = %s]]", get_hex(key).c_str());
        s >> set_size;
        set_size = letoh(set_size);
        // HOTSTUFF_LOG_INFO("[[Block UnSerialize : graph key-set size = %d]]", set_size);
        for(int i=0; i<set_size; i++){
            s >> set_element;
            graph[key].insert(set_element);
            // HOTSTUFF_LOG_INFO("[[Block UnSerialize : graph set val = %s]]", get_hex(set_element).c_str());
        }
    }

    /** unserialize e_update **/
    s >> n;
    n = letoh(n);
    e_update.resize(n);
    // HOTSTUFF_LOG_INFO("[[Block UnSerialize : e_update size = %d]]", e_update.size());
    for (auto &edge: e_update) {
        s >> edge.first;
        s >> edge.second;
        // HOTSTUFF_LOG_INFO("[[Block UnSerialize : e_update edge = %s, %s]]", get_hex(edge.first).c_str(), get_hex(edge.second).c_str());
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
