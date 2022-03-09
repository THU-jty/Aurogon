//
// Created by prince_de_marcia on 2021/3/25.
//

#ifndef CLOCK_MODEL_TABLE_H
#define CLOCK_MODEL_TABLE_H


#include "config.h"
#include "util.h"
#include <unordered_map>
#include <queue>

class Message{
public:
    MT message_type;
    ll ordering_ts;
};

class QueryMessage : public Message{
public:
    RW type;
    ll timestamp;
    ll txn_mg_id;
    ll txn_id;
    ll key;
    ll sz;
    ll value[VALUE_SIZE];
    ll req_id;
    ll source_node_id;
    ll source_thd_id;
    ll version_id;
    void init( MT _msg, RW _type, ll _timestamp, ll _txn_mg_id,
            ll _txn_id, ll _key, ll _sz, ll _value, ll _req_id, ll _s_id, ll _thd_id, ll o_ts );
    void copy( QueryMessage *msg );
    void output();
};

class AckMessage : public Message{
public:
    RW type;
    bool succeed;
    bool wait;
    ll txn_mg_id;
    ll txn_id;
    ll key;
    ll sz;
    ll value[VALUE_SIZE];
    ll timestamp;
    ll req_id;
    ll version_id;
    ll source_node_id;
    ll target_node_id;
    void init( MT _msg, RW _type, bool _s, ll _txn_mg_id,
            ll _txn_id, ll _key, ll _sz, ll _value, ll _req_id, ll o_ts );
    void output();
};

class CommitMessage : public Message{
public:
    RW type;
    bool succeed;
    ll txn_mg_id;
    ll txn_id;
    ll key;
    ll sz;
    ll value[VALUE_SIZE];
    ll timestamp;
    ll source_id;
    ll thd_id;
    ll req_id;
    ll version_id;
    void init( MT _msg, RW _type, bool _s, ll _s_id, ll _thd_id, ll _txn_mg_id,
            ll _txn_id, ll _key, ll _sz, ll _v, ll _tsp, ll _req_id, ll _v_id, ll o_ts );
    void output();
};

class MessageControlItem{
public:
    ll ts;
    char *ptr;
    ll id;
    bool local;
    int type;
    bool operator < ( const MessageControlItem &a )const{
        if( ts == a.ts ) return type > a.type;
        return ts > a.ts;
    }
};

class MessageControl{
public:
    ll cnt = 0;
    std::priority_queue< MessageControlItem > q;
    std::queue< MessageControlItem > q_bp;
    Queue my_q;
    Queue free_q;
    MessageControlItem *v;
    void init();
    void push( char* ptr, ll id, bool local );
    void pop(MessageControlItem *p);
};

class txn_item{
public:
    ll node_id;
    ll req_id;
    ll key;
    ll value;
    ll sz;
    ll req_status;
    ll req_v_id;
    bool last_one;
    txn_item(ll _node_id, ll _key, ll _value, ll _req_id, ll _sz):
            node_id(_node_id),key(_key),value(_value),req_id(_req_id),sz(_sz){}
    void output();
};

class row_item{
public:
    ll wts;
    ll rts;
    ll value;
};

class sta{
public:
    ll ts;
    ll now_ts;
    ll node_id;
    sta(ll _ts, ll _now_ts, ll _node_id):
        ts(_ts), now_ts(_now_ts), node_id(_node_id){}
};

class version{
public:
    RW type;
    ll timestamp;
    ll txn_mg_id;
    ll txn_id;
    ll s_id;
    ll thd_id;
    ll key;
    ll value[VALUE_SIZE];
    ll wts;
    ll rts;
    void init( QueryMessage *msg );
    void output();
};

class row{
public:
    int lock;
    Pri_lock prilock;
    ll key;
    Linklist lst, *rd_lst, wt_lst;

    row_item v[1];
    Queue write_q;
//    QueryMessage **read;
    version *write;
    Hotpoint hot;

    ll v_num;

    ll update_cnt;

    ll aborts[8];
    ll reqs[8];

    ll req_cnt;
    ll tranverse_cnt;

    ll req_wt_cnt;
    ll ins_cnt;
    int *txn_ids;
//    int pending[2];
//    std::vector<CommitMessage>log;
//    std::vector<Read_req_item>log2;
//    std::vector<version>log3;
//    std::vector<QueryMessage>log4;
//    std::vector<version>log5;
};

class mvcc_commit_ret;
class probe;

class Table{
public:
    std::unordered_map<ll,ll> mappings;
    ll row_cnt;
    row* rows;
    Zipfan_gen zipf;

//    std::atomic<ll> **max_ts;
    ll **max_ts;

    /*statistics*/
    std::vector< probe >sta;


    ll mapp( ll key );
    void init(ll _row_cnt);
    void read( QueryMessage *msg, AckMessage *ack_msg, ll thd_id );
    void write( QueryMessage *msg, AckMessage *ack_msg, ll thd_id );
    void rmw( QueryMessage *msg, AckMessage *ack_msg, ll thd_id );
    void commit_write( CommitMessage *cmsg );
    void mvcc_read( QueryMessage *msg, AckMessage *ack_msg, ll thd_id );
    void mvcc_write( QueryMessage *msg, AckMessage *ack_msg, ll thd_id );
    void mvcc_commit_write( CommitMessage *cmsg, ll thd_id, mvcc_commit_ret *ret );
//    void mvcc_commit_write( CommitMessage *cmsg, ll thd_id, AckMessage *ack_msg );
    void insert_read_req(ll thd_id, Read_req_item *item, ll item_id);
    void update_read_req(ll key, ll thd_id, mvcc_commit_ret *ret);
    void update_max_ts( ll node_id, ll thd_id, ll ts );
    void recycle_list( row* ro );
    ll get_buf_size( ll key );
};

class Request{
public:
    ll req_order;
    ll req_txnmger_id;
    ll req_type;
    // 1 read snapshot
    // 2 read-modify-write

    std::vector<txn_item> readreq;
    std::vector<txn_item> writereq;
    std::vector<txn_item> rmwreq;
    ll lat;
    ll st, ed;
    ll abort_cnt;
    bool need_retry;
    bool in_flight;
    void init(){
        for( ll i = 0; i < readreq.size(); i ++ ){
            readreq[i].req_status = 0;
            readreq[i].req_v_id = -1;
        }
        for( ll i = 0; i < writereq.size(); i ++ ){
            writereq[i].req_status = 0;
            writereq[i].req_v_id = -1;
        }
        for( ll i = 0; i < rmwreq.size(); i ++ ){
            rmwreq[i].req_status = 0;
            rmwreq[i].req_v_id = -1;
        }
    }
    void tag(){
        for( ll i = 0; i < readreq.size(); i ++ ){
            readreq[i].last_one = 0;
        }
        for( ll i = 0; i < writereq.size(); i ++ ){
            writereq[i].last_one = 0;
        }
        for( ll i = 0; i < rmwreq.size(); i ++ ){
            rmwreq[i].last_one = 0;
        }
        bool node_c[20];
        memset( node_c, 0, sizeof(node_c) );
        for( ll i = readreq.size()-1; i >= 0; i -- ){
            if( node_c[ readreq[i].node_id ] == 0 ){
                readreq[i].last_one = 1;
                node_c[ readreq[i].node_id ] = 1;
            }
        }
        memset( node_c, 0, sizeof(node_c) );
        for( ll i = writereq.size()-1; i >= 0; i -- ){
            if( node_c[ writereq[i].node_id ] == 0 ){
                writereq[i].last_one = 1;
                node_c[ writereq[i].node_id ] = 1;
            }
        }
        memset( node_c, 0, sizeof(node_c) );
        for( ll i = rmwreq.size()-1; i >= 0; i -- ){
            if( node_c[ rmwreq[i].node_id ] == 0 ){
                rmwreq[i].last_one = 1;
                node_c[ rmwreq[i].node_id ] = 1;
            }
        }
    }
};

class mvcc_commit_ret{
public:
    std::vector<ll> local;
    std::vector< std::pair<ll,ll> > remote;
};

class probe{
public:
    ll now_ts, now_node, con_ts, con_node;
    probe(ll _now_ts, ll _now_node, ll _con_ts, ll _con_node):
            now_ts(_now_ts),now_node(_now_node),con_ts(_con_ts),con_node(_con_node){}

};

#endif //CLOCK_MODEL_TABLE_H
