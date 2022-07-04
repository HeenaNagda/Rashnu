#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include "hotstuff/graph.h"
#include "hotstuff/entity.h"


using namespace hotstuff;
using hotstuff::uint256_t;

#define LOG_INFO HOTSTUFF_LOG_INFO
#define LOG_DEBUG HOTSTUFF_LOG_DEBUG
#define LOG_WARN HOTSTUFF_LOG_WARN
#define LOG_PROTO HOTSTUFF_LOG_PROTO
#define IS_TRUE(x) { if (!(x)) std::cout << __FUNCTION__ << " failed on line " << __LINE__ << std::endl; }

void test1(){
    int arr[] = { 1, 1, 2, 1, 1, 3, 4, 3 };
    int n = sizeof(arr) / sizeof(arr[0]);
 
    std::unordered_map<int, int> m;
    for (int i = 0; i < n; i++)
        m[arr[i]]++;
 
    HOTSTUFF_LOG_WARN("Element  Frequency");
    for (auto i : m)
        HOTSTUFF_LOG_WARN("%d %d", i.first, i.second);
 



    std::unordered_map<uint256_t, std::unordered_set<uint256_t>> graph;
    uint256_t* n3 = uint256_new();
    // uint256_t n0 = salticidae::get_hash(0);
    uint256_t n1 = salticidae::get_hash(1);
    // uint256_t n2 = salticidae::get_hash(2);
    // uint256_t n3 = salticidae::get_hash(3);
    // uint256_t n4 = salticidae::get_hash(4);

    DataStream s0; s0 << 0; uint256_t n0 = s0.get_hash();
    DataStream s2; s2 << 1; uint256_t n2 = s2.get_hash();


    HOTSTUFF_LOG_WARN("======== all NODES =======");
    HOTSTUFF_LOG_WARN("%x %x %x %x", n0.to_bytes(), n0.to_bytes(), n0.to_hex(), n0.to_hex());

    std::unordered_set<uint256_t> n0set;
    n0set.insert(n2);
    graph.insert(std::make_pair(n0, n0set));
    // graph[n0].insert(n2);
    HOTSTUFF_LOG_WARN("======== INPUT graph %d =======", graph.size());
    for(auto const& [key,value]: graph){
        HOTSTUFF_LOG_WARN("[[ %x ]]", key);
        HOTSTUFF_LOG_WARN("{");
        for(auto const& child: graph[key]){
            HOTSTUFF_LOG_WARN("%x", child);
        }
        HOTSTUFF_LOG_WARN("}");
    }


    // graph[n0].insert(n3);
    // HOTSTUFF_LOG_WARN("======== INPUT graph %d =======", graph.size());
    // for(auto const g: graph){
    //     HOTSTUFF_LOG_WARN("[[ %x ]]", g.first);
    //     HOTSTUFF_LOG_WARN("{");
    //     for(auto const child: g.second){
    //         HOTSTUFF_LOG_WARN("%x", child);
    //     }
    //     HOTSTUFF_LOG_WARN("}");
    // }

    // graph[n1].insert(n0);
    // HOTSTUFF_LOG_WARN("======== INPUT graph %d =======", graph.size());
    // for(auto const g: graph){
    //     HOTSTUFF_LOG_WARN("[[ %x ]]", g.first);
    //     HOTSTUFF_LOG_WARN("{");
    //     for(auto const child: g.second){
    //         HOTSTUFF_LOG_WARN("%x", child);
    //     }
    //     HOTSTUFF_LOG_WARN("}");
    // }

    // graph[n2] = std::unordered_set<uint256_t>();
    // HOTSTUFF_LOG_WARN("======== INPUT graph %d =======", graph.size());
    // for(auto const g: graph){
    //     HOTSTUFF_LOG_WARN("[[ %x ]]", g.first);
    //     HOTSTUFF_LOG_WARN("{");
    //     for(auto const child: g.second){
    //         HOTSTUFF_LOG_WARN("%x", child);
    //     }
    //     HOTSTUFF_LOG_WARN("}");
    // }

    // graph[n3].insert(n4);
    // HOTSTUFF_LOG_WARN("======== INPUT graph %d =======", graph.size());
    // for(auto const g: graph){
    //     HOTSTUFF_LOG_WARN("[[ %x ]]", g.first);
    //     HOTSTUFF_LOG_WARN("{");
    //     for(auto const child: g.second){
    //         HOTSTUFF_LOG_WARN("%x", child);
    //     }
    //     HOTSTUFF_LOG_WARN("}");
    // }

    // graph[n4] = std::unordered_set<uint256_t>();
    // HOTSTUFF_LOG_WARN("======== INPUT graph %d =======", graph.size());
    // for(auto const g: graph){
    //     HOTSTUFF_LOG_WARN("[[ %x ]]", g.first);
    //     HOTSTUFF_LOG_WARN("{");
    //     for(auto const child: g.second){
    //         HOTSTUFF_LOG_WARN("%x", child);
    //     }
    //     HOTSTUFF_LOG_WARN("}");
    // }

    
    // HOTSTUFF_LOG_WARN("======== INPUT graph %d =======", graph.size());
    // for(auto const g: graph){
    //     HOTSTUFF_LOG_WARN("[[ %x ]]", g.first);
    //     HOTSTUFF_LOG_WARN("{");
    //     for(auto const child: g.second){
    //         HOTSTUFF_LOG_WARN("%x", child);
    //     }
    //     HOTSTUFF_LOG_WARN("}");
    // }

    CondensationGraph condensationGraph = CondensationGraph(graph);
    std::vector<std::vector<uint256_t>> sccs = condensationGraph.get_condensation_graph();

    int i=1;
    for(std::vector<uint256_t> scc: sccs){
        HOTSTUFF_LOG_WARN("======== scc [%d] START =======", i);
        for(uint256_t node: scc){
            HOTSTUFF_LOG_WARN("%x ",node);
        }
        HOTSTUFF_LOG_WARN("======== scc [%d] END =======", i);
        i++;
    }

}

int main(){
    test1();
    return 0;
}
