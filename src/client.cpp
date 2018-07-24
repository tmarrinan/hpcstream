#include "hpcstream/client.h"

HpcStream::Client::Client(const char *host, uint16_t port, MPI_Comm comm)
{
    MPI_Comm_dup(comm, &_comm);
    int rc = MPI_Comm_rank(_comm, &_rank);
    rc |= MPI_Comm_size(_comm, &_num_ranks);
    if (rc != 0) {
        fprintf(stderr, "Error obtaining MPI task ID information\n");
    }

    // data storage checks - little endian, ieee 754
    uint32_t int_test = 0x00000001;
    uint8_t int_bin = *reinterpret_cast<uint8_t*>(&int_test);
    if (int_bin == 1)
    {
        _endianness = HpcStream::Endian::Little;
    }
    else
    {
        _endianness = HpcStream::Endian::Big;
    }
    double float_test = HPCSTREAM_FLOATTEST;
    int64_t float_bin = *reinterpret_cast<uint64_t*>(&float_test);
    if (float_bin != HPCSTREAM_FLOATBINARY)
    {
        fprintf(stderr, "Warning: machine does not appear to use IEEE 754 floating point format\n");
    }

    // netsocket client connections
    printf("[rank %d] begin...\n", _rank);
    NetSocket::ClientOptions options = NetSocket::CreateClientOptions();
    options.secure = false;
    // rank 0 receives host/port info for all other ranks
    int i;
    uint8_t *remote_ip_addresses;
    uint16_t *remote_ports;
    if (_rank == 0)
    {
        Connection c;
        c.client = new NetSocket::Client(host, port, options);
        _connections.push_back(c);
        int received_server_info = 0;
        while (received_server_info < 3)
        {
            NetSocket::Client::Event event = c.client->WaitForNextEvent();
            switch (event.type)
            {
                case NetSocket::Client::EventType::ReceiveBinary:
                    if (received_server_info == 0)      // endianness
                    {
                        HpcStream::Endian remote_endianness = (HpcStream::Endian)(*((uint8_t*)event.binary_data));
                        if (remote_endianness != _endianness)
                        {
                            fprintf(stderr, "Warning: remote machine's endianness does not match\n");
                        }
                        delete[] event.binary_data;
                        received_server_info++;
                    }
                    else if (received_server_info == 1) // ip addresses
                    {
                        _num_remote_ranks = event.data_length / 4;
                        remote_ip_addresses = (uint8_t*)event.binary_data;
                        received_server_info++;
                    }
                    else                                // ports
                    {
                        remote_ports = (uint16_t*)event.binary_data;
                        for (i = 0; i<_num_remote_ranks; i++)
                        {
                            remote_ports[i] = ntohs(remote_ports[i]);
                        }
                        received_server_info++;
                    }
                    break;
                default:
                    break;
            }
        }
    }
    // share info with other ranks
    MPI_Bcast(&_num_remote_ranks, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (_rank != 0)
    {
        remote_ip_addresses = new uint8_t[4 * _num_remote_ranks];
        remote_ports = new uint16_t[_num_remote_ranks];
    }
    MPI_Bcast(remote_ip_addresses, 4 * _num_remote_ranks, MPI_UINT8_T, 0, MPI_COMM_WORLD);
    MPI_Bcast(remote_ports, _num_remote_ranks, MPI_UINT16_T, 0, MPI_COMM_WORLD);
    // determine which ranks connect to which
    int connections_per_rank = _num_remote_ranks / _num_ranks;
    int connections_extra = _num_remote_ranks % _num_ranks;
    int num_connections = connections_per_rank + (_rank < connections_extra ? 1 : 0);
    int connection_offset = _rank * connections_per_rank + std::min(_rank, connections_extra);
    // make connections
    printf("[rank %d] about to connect\n", _rank);
    for (i = std::max(connection_offset, 1); i < connection_offset + num_connections; i++)
    {
        struct in_addr addr = {*((in_addr_t*)(&(remote_ip_addresses[4*i])))};
        Connection c;
        c.client = new NetSocket::Client(inet_ntoa(addr), remote_ports[i], options);
        NetSocket::Client::Event event = c.client->WaitForNextEvent();
        while (event.type != NetSocket::Client::EventType::Connect)
        {
            if (event.type == NetSocket::Client::EventType::ReceiveBinary)
            {
                delete[] event.binary_data;
            }
            event = c.client->WaitForNextEvent();
        }
        _connections.push_back(c);
    }
    // create handshake data
    printf("[rank %d] about to send handshake data\n", _rank);
    uint8_t info_received[21];
    if (_rank == 0)
    {
        uint32_t *info_remote_ranks = (uint32_t*)info_received;
        uint64_t *info_id = (uint64_t*)((uint32_t*)info_received + 1);
        uint32_t *info_ranks = (uint32_t*)info_received + 4;
        *info_remote_ranks = htonl(_num_remote_ranks);
        struct in_addr ip;
        uint16_t port;
        inet_aton(_connections[0].client->LocalIpAddress().c_str(), &ip);
        port = htons(_connections[0].client->LocalPort());
        memcpy(info_id, &ip, 4);
        memcpy(info_id + 4, &port, 2);
        *info_ranks = htonl(_num_ranks);
    }
    MPI_Bcast(info_received, 21, MPI_UINT8_T, 0, MPI_COMM_WORLD);
    uint32_t *info_rank = (uint32_t*)info_received + 3;
    *info_rank = htonl(_rank);
    info_received[20] = _endianness;
    for (i = 0; i < num_connections; i++)
    {
        _connections[i].client->Send(info_received, 21, NetSocket::CopyMode::MemCopy);
    }
    // receive variable declarations
    printf("[rank %d] about to receive var decl\n", _rank);
    for (i = 0; i < num_connections; i++)
    {
        NetSocket::Client *c = _connections[i].client;
        bool received_vars = false;
        while (!received_vars)
        {
            NetSocket::Client::Event event = c->WaitForNextEvent();
            uint32_t vars_offset = 0;
            uint8_t* data;
            switch (event.type)
            {
                case NetSocket::Client::EventType::ReceiveBinary:
                    data = (uint8_t*)event.binary_data;
                    while (vars_offset < event.data_length)
                    {
                        SharedVar v;
                        uint32_t var_name_len = ntohl(*((uint32_t*)(data + vars_offset)));
                        vars_offset += sizeof(uint32_t);
                        std::string var_name = std::string((char*)(data + vars_offset), var_name_len);
                        vars_offset += var_name_len;
                        v.dims = ntohl(*((uint32_t*)(data + vars_offset)));
                        vars_offset += sizeof(uint32_t);
                        v.type = (HpcStream::DataType)(*((uint8_t*)(data + vars_offset)));
                        vars_offset += sizeof(uint8_t);
                        v.size = ntohl(*((uint32_t*)(data + vars_offset)));
                        vars_offset += sizeof(uint32_t);
                        v.length = ntohll(*((int64_t*)(data + vars_offset)));
                        vars_offset += sizeof(int64_t);
                        if (v.length == 0)
                        {
                            int j;
                            uint32_t len;
                            v.g_size = new uint32_t[v.dims];
                            v.l_size = new uint32_t[v.dims];
                            v.l_offset = new uint32_t[v.dims];
                            for (j = 0; j < v.dims; j++)
                            {
                                len = ntohl(*((uint32_t*)(data + vars_offset)));
                                vars_offset += sizeof(uint32_t);
                                std::string gs = std::string((char*)(data + vars_offset), len);
                                vars_offset += len;
                                v.gs_vars.push_back(gs);
                            }
                            for (j = 0; j < v.dims; j++)
                            {
                                len = ntohl(*((uint32_t*)(data + vars_offset)));
                                vars_offset += sizeof(uint32_t);
                                std::string ls = std::string((char*)(data + vars_offset), len);
                                vars_offset += len;
                                v.ls_vars.push_back(ls);
                            }
                            for (j = 0; j < v.dims; j++)
                            {
                                len = ntohl(*((uint32_t*)(data + vars_offset)));
                                vars_offset += sizeof(uint32_t);
                                std::string lo = std::string((char*)(data + vars_offset), len);
                                vars_offset += len;
                                v.lo_vars.push_back(lo);
                            }
                        }
                        printf("[rank %d] client %d: var = %s, %u (t=%u)\n", _rank, i, var_name.c_str(), v.dims, v.type);
                        _connections[i].vars[var_name] = v;
                    }
                    received_vars = true;
                    break;
                default:
                    break;
            }
        }
    }
}

HpcStream::Client::~Client()
{
}
