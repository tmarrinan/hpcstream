#ifndef __HPCSTREAM_CLIENT_H_
#define __HPCSTREAM_CLIENT_H_

#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <thread>
#include <mpi.h>
extern "C" {
#include <ddr.h>
}
#include <netsocket/client.h>
#include "hpcstream.h"

class HpcStream::Client {
private:
    typedef struct SharedVar {
        HpcStream::DataType type;         // base data type for each element
        uint32_t dims;                    // array dimensions
        std::vector<std::string> gs_vars; // names of vars to define global size of array
        std::vector<std::string> ls_vars; // names of vars to define local size of array 
        std::vector<std::string> lo_vars; // name of vars to define local offset into the global space
        uint32_t *g_size;                 // array of global array sizes
        uint32_t *l_size;                 // array of local array sizes
        uint32_t *l_offset;               // array of local array offsets
        uint8_t *val;                     // byte buffer for variable value(s)
        uint32_t size;                    // size of single element (bytes)
        int64_t length;                   // number of local elements
    } SharedVar;
    typedef struct Connection {
        NetSocket::Client* client;
        std::map<std::string, SharedVar> vars;
    } Connection;

    int _rank;
    int _num_ranks;
    int _num_remote_ranks;
    MPI_Comm _comm;
    HpcStream::Endian _endianness;
    std::vector<Connection> _connections;

    void ConnectionRead(int connection_idx);

public:
    typedef struct GlobalSelection {
        std::string var_name;
        DDR_DataDescriptor *desc;
    } GlobalSelection;

    Client(const char *iface, uint16_t port, MPI_Comm comm);
    ~Client();

    void Read();
    void ReleaseTimeStep();
    void GetGlobalSizeForVariable(std::string var_name, uint32_t *size);
    GlobalSelection CreateGlobalArraySelection(std::string var_name, int32_t *sizes, int32_t *offsets);
    void FillSelection(GlobalSelection& selection, void *data);
};

#endif // __HPCSTREAM_CLIENT_H_