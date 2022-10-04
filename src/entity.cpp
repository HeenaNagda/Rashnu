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
    GraphFormatConversion *format = new GraphFormatConversion();
    format->serialize(s, graph);

    /** Serialize e_missing **/
    s << htole((uint32_t)e_missing.size());
    for (auto edge: e_missing){
        s << edge.first;
        s << edge.second;
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
    GraphFormatConversion *format = new GraphFormatConversion();
    format->unserialize(s, graph);

    /** unserialize e_missing **/
    s >> n;
    n = letoh(n);
    e_missing.resize(n);
    for (auto &edge: e_missing) {
        s >> edge.first;
        s >> edge.second;
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
