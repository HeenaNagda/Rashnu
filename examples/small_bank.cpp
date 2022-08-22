/**
 * Author: Heena
 * 
 */

#include "small_bank.h"


SmallBank::SmallBank(uint64_t n_users){
    this->n_users = n_users;
    
    /* initialize random seed: */
    srand (time(NULL));
    uint64_t const max_amount = 10000;

    /* Initialize accounts with random amount */
    for(uint64_t user_id=0; user_id<n_users; user_id++){
        checking_accounts.push_back(rand()%max_amount);
        saving_accounts.push_back(rand()%max_amount);
    }
}

void SmallBank::transaction_savings(uint64_t user_id, uint64_t amount){
    if(UINT64_MAX-saving_accounts[user_id] < amount){
        /* If amount is exceded to the maximum limit of uint64_t : discart transaction */
        return;
    }
    saving_accounts[user_id] += amount;
}

void SmallBank::deposit_checking(uint64_t user_id, uint64_t amount){
    if(UINT64_MAX-checking_accounts[user_id] < amount){
        /* If amount is exceded to the maximum limit of uint64_t : discart transaction */
        return;
    }
    checking_accounts[user_id] += amount;
}

void SmallBank::write_check(uint64_t user_id, uint64_t amount){
    if(checking_accounts[user_id]<amount){
        /* If the user has not sufficient amount in the checking account : discart transaction */
        return;
    }
    checking_accounts[user_id] -= amount;
}

void SmallBank::amalgamate(uint64_t user_id){
    if(UINT64_MAX-checking_accounts[user_id] < saving_accounts[user_id]){
        /* If amount is exceded to the maximum limit of uint64_t : discart transaction */
        return;
    }
    checking_accounts[user_id] += saving_accounts[user_id];
    saving_accounts[user_id] = 0;
}

void SmallBank::send_payment(uint64_t from_user_id, uint64_t to_user_id, uint64_t amount){
    if(checking_accounts[from_user_id]<amount
        || UINT64_MAX-checking_accounts[to_user_id] < amount){
        /* If the from_user_id has not sufficient amount in the checking account OR */
        /* If to_user_id amount is exceded to the maximum limit of uint64_t : discart transaction */
        return;
    }
    checking_accounts[from_user_id] -= amount;
    checking_accounts[to_user_id] += amount;
}

void SmallBank::transaction_split(std::vector<std::pair<uint64_t, uint64_t>> payors, std::vector<uint64_t> party){
    auto party_size = party.size();
    auto amount_payed=0;
    for(auto payor: payors){
        amount_payed += payor.second;
    }
    auto per_head_amount_to_pay = amount_payed/party.size();

    /* validity check */
    for(auto payor: payors){
        if(checking_accounts[payor.first]<payor.second){
            /* If the user has not sufficient amount in the checking account : discart transaction */
            return;
        }
    }
    for(auto user_id: party){
        if(UINT64_MAX-checking_accounts[user_id] < per_head_amount_to_pay){
            /* If amount is exceded to the maximum limit of uint64_t : discart transaction */
            return;
        }
    }

    /* Do transaction */
    for(auto payor: payors){
        checking_accounts[payor.first] += payor.second;
    }
    for(auto user_id: party){
        checking_accounts[user_id] -= per_head_amount_to_pay;
    }

}

std::pair<uint64_t,uint64_t> SmallBank::query(uint64_t user_id){
    return std::make_pair(checking_accounts[user_id], saving_accounts[user_id]);
}

SmallBankManager::SmallBankManager(uint64_t n_users, double prob_choose_mtx, double skew_factor){
    this->bank = new SmallBank(n_users);

    this->n_users = n_users;
    this->prob_choose_mtx = prob_choose_mtx;

    /* initialize random seed: */
    srand (time(NULL));

    tx_distribution = std::bernoulli_distribution(prob_choose_mtx);

    random_mtx = std::make_pair(0,TX_TYPES-2);
    mtx_distribution = std::uniform_int_distribution<uint64_t>(
                                        random_mtx.first, 
                                        random_mtx.second);

    random_users = std::make_pair(0,n_users-1);
    user_distribution = zipfian_int_distribution<uint64_t>(
                                        random_users.first,
                                        random_users.second,
                                        skew_factor);
    
    // printf("*****************[ Choosing mtx ]******************\n");
    // int count=0;  // count number of trues
    // for (int i=0; i<10000; ++i) if (tx_distribution(tx_generator)) ++count;
    // printf("[Choosing mtx: %d] [Not Choosing mtx: %d]\n", count, 10000-count );

    // printf("*****************[ mtx distribution ]******************\n");
    // for(int i=0; i<=9; i++){
    //     printf("[Iteration: %d] [Tx num: %ld]\n", i, mtx_distribution(mtx_generator));
    // }

    // printf("*****************[ u distribution ]******************\n");
    // for(int i=10; i<=20; i++){
    //     printf("[Iteration: %d] [User num: %ld]\n", i-10, user_distribution(user_generator));
    // }
}

std::vector<uint64_t> SmallBankManager::get_next_transaction_serialized(){
    uint64_t tx_type;
    uint64_t user_id;

    /** Find the next transaction type **/
    if(tx_distribution(tx_generator)){
        /* Modifying transactions are chosen */
        tx_type = mtx_distribution(mtx_generator);
    }
    else{
        /* Query transaction is chosen */
        tx_type = TX_TYPES-1;
    }

    return get_next_transaction_by_type(tx_type);
}

std::vector<uint64_t> SmallBankManager::get_next_transaction_by_type(uint64_t tx_type){
    std::vector<uint64_t> tx_payload;
    tx_payload.push_back(tx_type); 

    switch (tx_type)
    {
        case 0:{
            /** void transaction_savings(uint64_t user_id, uint64_t amount) **/
            /** payload format : [tx type, user id, amount] **/

            /* find next user_id */
            auto user_id = user_distribution(user_generator);
            tx_payload.push_back(user_id);
            /* find random saving amount */
            auto saving_amount = bank->query(user_id).second;
            auto amount = random_number_generator(0,(UINT64_MAX-saving_amount)/2);
            tx_payload.push_back(amount);
            
            break;
        }
            
        
        case 1:{
            /** void deposit_checking(uint64_t user_id, uint64_t amount) **/
            /** payload format : [tx type, user id, amount] **/

            /* find next user_id */
            auto user_id = user_distribution(user_generator);
            tx_payload.push_back(user_id);
            /* find random checking amount */
            auto checking_amount = bank->query(user_id).first;
            auto amount = random_number_generator(0,(UINT64_MAX-checking_amount)/2);
            tx_payload.push_back(amount);
            
            break;
        }
            
        
        case 2:{
            /** void write_check(uint64_t user_id, uint64_t amount) **/
            /** payload format : [tx type, user id, amount] **/

            /* find next user_id */
            auto user_id = user_distribution(user_generator);
            tx_payload.push_back(user_id);
            /* find random checking amount */
            auto checking_amount = bank->query(user_id).first;
            auto amount = random_number_generator(0,checking_amount);
            tx_payload.push_back(amount);
            
            break;
        }
            
        
        case 3:{
            /** void send_payment(uint64_t from_user_id, uint64_t to_user_id, uint64_t amount) **/
            /** payload format : [tx type, from user id, to user id, amount] **/

            /* find next user_id */
            auto from_user_id = user_distribution(user_generator);
            auto to_user_id = user_distribution(user_generator);
            tx_payload.push_back(from_user_id);
            tx_payload.push_back(to_user_id);
            /* find random checking amount */
            auto from_checking_amount = bank->query(from_user_id).first;
            auto to_checking_amount = bank->query(to_user_id).first;
            auto amount = random_number_generator(0,std::min<uint64_t>(from_checking_amount, UINT64_MAX-to_checking_amount));
            tx_payload.push_back(amount);
            
            break; 
        }
            

        case 4:{
            /** void transaction_split(std::vector<std::pair<uint64_t, uint64_t>> payors, std::vector<uint64_t> party) **/
            /** payload format : [tx type, count of payors, payer_1 id, payer_1 amount, ..., party size, member_1, member_1, ...] **/

            /** Find the next party size users **/
            uint64_t party_size = random_number_generator(MIN_SPLIT_TX_PARTY_SIZE, MAX_SPLIT_TX_PARTY_SIZE);
            uint64_t n_payors = random_number_generator(1, party_size/2);
            uint64_t user_id_start = user_distribution(user_generator);
            while(user_id_start+party_size > n_users){
                user_id_start = user_distribution(user_generator);
            }

            /** find the max amount that can be split **/
            uint64_t max_amount = UINT64_MAX;
            for(uint64_t user_id=user_id_start; user_id<user_id_start+n_payors; user_id++){
                max_amount = std::min(max_amount, bank->query(user_id).first);
            }
            for(uint64_t user_id=user_id_start+n_payors; user_id<user_id_start+party_size; user_id++){
                max_amount = std::min(max_amount, UINT64_MAX - bank->query(user_id).first);
            }
            
            /* update transaction payload */
            tx_payload.push_back(n_payors);
            for(uint64_t user_id=user_id_start; user_id<user_id_start+n_payors; user_id++){
                tx_payload.push_back(user_id);
                uint64_t amount = random_number_generator(0,max_amount/n_payors);
                tx_payload.push_back(amount);
            }

            tx_payload.push_back(party_size);
            for(uint64_t user_id=user_id_start; user_id<user_id_start+party_size; user_id++){
                tx_payload.push_back(user_id);
            }
            
            break;
        }
            

        case 5:{
            /** void amalgamate(uint64_t user_id) **/
            /** payload format : [tx type, user id] **/

            /* find next user_id */
            auto user_id = user_distribution(user_generator);
            tx_payload.push_back(user_id);
            
            break;
        }
            

        case 6:{
            /** std::pair<uint64_t,uint64_t> query(uint64_t user_id) **/
            /** payload format : [tx type, user id] **/

            /* find next user_id */
            auto user_id = user_distribution(user_generator);
            tx_payload.push_back(user_id);
            
            break;
        }   

        default:
            fprintf(stderr, "Wrong transaction type at the time of serialization");
        break;
    }
    return tx_payload;
}

uint64_t SmallBankManager::random_number_generator(uint64_t min, uint64_t max){
    return (rand() % (max-min+1)) + min;
}


std::pair<uint64_t, uint64_t> SmallBankManager::execute_transaction(const uint64_t* tx_payload){
    std::pair<uint64_t, uint64_t> accounts = std::make_pair(0,0);
    size_t idx = 0;
    idx++;

    switch (tx_payload[0])
    {
        case 0:{
            /** void transaction_savings(uint64_t user_id, uint64_t amount) **/
            /** payload format : [tx type, user id, amount] **/
            
            auto user_id = tx_payload[idx++];
            auto amount = tx_payload[idx++];   
            bank->transaction_savings(user_id, amount);   

            break;
        }
            
        
        case 1:{
            /** void deposit_checking(uint64_t user_id, uint64_t amount) **/
            /** payload format : [tx type, user id, amount] **/

            auto user_id = tx_payload[idx++];
            auto amount = tx_payload[idx++];
            bank->deposit_checking(user_id, amount);
            
            break;
        }
            
        
        case 2:{
            /** void write_check(uint64_t user_id, uint64_t amount) **/
            /** payload format : [tx type, user id, amount] **/

            auto user_id = tx_payload[idx++];
            auto amount = tx_payload[idx++];
            bank->write_check(user_id,amount);
            
            break;
        }
            
        
        case 3:{
            /** void send_payment(uint64_t from_user_id, uint64_t to_user_id, uint64_t amount) **/
            /** payload format : [tx type, from user id, to user id, amount] **/

            auto from_user_id = tx_payload[idx++];
            auto to_user_id = tx_payload[idx++];
            auto amount = tx_payload[idx++];
            bank->send_payment(from_user_id, to_user_id, amount);
            
            break; 
        }
            

        case 4:{
            /** void transaction_split(std::vector<std::pair<uint64_t, uint64_t>> payors, std::vector<uint64_t> party) **/
            /** payload format : [tx type, count of payors, payer_1 id, payer_1 amount, ..., party size, member_1, member_1, ...] **/

            auto n_payors = tx_payload[idx++];

            std::vector<std::pair<uint64_t, uint64_t>> payors; 
            for(int i=0; i<n_payors; i++){
                auto payor_id = tx_payload[idx++];
                auto payor_amount = tx_payload[idx++];
                payors.push_back(std::make_pair(payor_id, payor_amount));
            }
            
            auto party_size = tx_payload[idx++];

            std::vector<uint64_t> party;
            for(int i=0; i<party_size; i++){
                auto user_id = tx_payload[idx++];
                party.push_back(user_id);
            }

            bank->transaction_split(payors, party);
            
            break;
        }
            

        case 5:{
            /** void amalgamate(uint64_t user_id) **/
            /** payload format : [tx type, user id] **/

            auto user_id = tx_payload[idx++];
            bank->amalgamate(user_id);
            
            break;
        }
            

        case 6:{
            /** std::pair<uint64_t,uint64_t> query(uint64_t user_id) **/
            /** payload format : [tx type, user id] **/

            auto user_id = tx_payload[idx++];
            accounts = bank->query(user_id);
            
            break;
        }  
        
        default:
            fprintf(stderr, "Wrong transaction type received at the time of execution");
            break;
    }

    return accounts;
}

std::pair<uint64_t,uint64_t> SmallBankManager::show_account_info(uint64_t user_id){
    return bank->query(user_id);
}
 

/******************************** Test Small Bank /********************************/
// int main(){
//     // uint64_t n = 11;
//     // SmallBank *bank = new SmallBank(n);

//     // for(uint64_t i=0; i<n; i++){
//     //     printf("[User: %ld] [C: %ld] [S: %ld]\n", i, bank->query(i).first, bank->query(i).second);
//     // }

//     // bank->transaction_savings(0, 10);
//     // bank->deposit_checking(1,10);
//     // bank->send_payment(2,3,10);
//     // bank->write_check(4, 10);
//     // bank->amalgamate(5);
//     // std::vector<std::pair<uint64_t,uint64_t>> payors{std::make_pair(6,30), std::make_pair(7,20)};
//     // std::vector<uint64_t> party{6,7,8,9,10};
//     // bank->transaction_split(payors, party);

//     // printf("\nAfter Transactions :\n\n");
//     // for(uint64_t i=0; i<n; i++){
//     //     printf("[User: %ld] [C: %ld] [S: %ld]\n", i, bank->query(i).first, bank->query(i).second);
//     // }

//     // uint64_t n = 11;
//     // double pm = 0.5;
//     // uint64_t skew = 0.99; 
//     // SmallBankManager *manager = new SmallBankManager(n, pm, skew);

//     // for(uint64_t i=0; i<n; i++){
//     //     printf("[User: %ld] [C: %ld] [S: %ld]\n", i, manager->show_account_info(i).first, manager->show_account_info(i).second);
//     // }

//     // for(uint64_t tx_type=0; tx_type<7; tx_type++){

//     //     for(int i=0; i<10; i++){
//     //         printf("\n[TX %ld] ", tx_type);
//     //         auto payload = manager->get_next_transaction_by_type(tx_type);
//     //         for(auto element: payload){
//     //             printf("[ %ld ] ", element);
//     //         }
//     //     }
//     //     printf("\n-------------------------------\n");
//     // }

//     // for(int i=0; i<10; i++){
//     //     auto payload = manager->get_next_transaction_serialized();
//     //     for(auto element: payload){
//     //         printf("[ %ld ] ", element);
//     //     }
//     //     manager->execute_transaction(&payload[0]);
//     //     printf("\n");
//     //     printf("[User: %ld] [C: %ld] [S: %ld]\n", payload[1], manager->show_account_info(payload[1]).first, manager->show_account_info(payload[1]).second);
//     // }
    
    

//     // printf("\n");
//     return 0;
// }