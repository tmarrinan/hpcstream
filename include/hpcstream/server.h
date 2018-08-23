#ifndef __HPCSTREAM_SERVER_H_
#define __HPCSTREAM_SERVER_H_

#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <map>
#include <algorithm>
#include <random>
#include <ifaddrs.h>
#include <mpi.h>
#include <netsocket/server.h>
#include "hpcstream.h"

class HpcStream::Server {
public:
    enum StreamBehavior : uint8_t {WaitForAll, DropFrames};

private:
    enum ClientState : uint8_t {Connecting, Handshake, Streaming, Finished};
    typedef struct SharedVar {
        HpcStream::DataType type;         // base data type for each element
        uint32_t dims;                    // array dimensions
        std::vector<std::string> gs_vars; // names of vars to define global size of array
        std::vector<std::string> ls_vars; // names of vars to define local size of array 
        std::vector<std::string> lo_vars; // name of vars to define local offset into the global space
        uint32_t *g_size;                 // array of global array sizes
        uint32_t *l_size;                 // array of local array sizes
        uint32_t *l_offset;               // array of local array offsets
        uint8_t *send_buf;                // byte buffer for variable name and value(s)
        uint8_t *val;                     // byte buffer for variable value(s)
        uint32_t size;                    // size of single element (bytes)
        int64_t length;                   // number of local elements
        bool updated;                     // whether or not the variable has been updated since last send
    } SharedVar;
    typedef struct Connection {
        uint64_t id;
        ClientState state;
        NetSocket::ClientConnection::Pointer client;
        int remote_rank;
        int num_remote_ranks;
        bool is_new;
        bool has_same_endianness;
    } Connection;

    int _rank;
    int _num_ranks;
    uint16_t _port;
    MPI_Comm _comm;
    uint8_t *_ip_address_list;
    uint16_t *_port_list;
    StreamBehavior _stream_behavior;
    int _initial_client_count;
    int _num_connections;
    HpcStream::Endian _endianness;
    uint32_t _vars_buffer_size;
    uint8_t *_vars_buffer;
    std::map<std::string, SharedVar> _vars;
    std::map<std::string, Connection> _connections;
    NetSocket::Server *_server;

    void GenerateVarsBuffer();
    bool HandleNewConnection(NetSocket::Server::Event& event);
    void GetIpAddress(const char *iface, uint8_t ip_address[4]);
    std::vector<std::string> ParseVarCounts(std::string counts);

public:
    Server(const char *iface, uint16_t port_min, uint16_t port_max, MPI_Comm comm);
    ~Server();

    char* GetMasterIpAddress();
    uint16_t GetMasterPort();
    void DefineVar(std::string name, HpcStream::DataType base_type, std::string global_size, std::string local_size, std::string local_offset);
    void VarDefinitionsComplete(StreamBehavior behavior, int initial_wait_count);
    void SetValue(std::string name, void *value);
    void Write();
    void AdvanceTimeStep();
};

#endif // __HPCSTREAM_SERVER_H_