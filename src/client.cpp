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
    uint8_t info_received[21];
    if (_rank == 0)
    {
        uint32_t *info_remote_ranks = (uint32_t*)info_received;
        uint64_t *info_id = (uint64_t*)((uint32_t*)info_received + 1);
        uint32_t *info_ranks = (uint32_t*)info_received + 4;
        *info_remote_ranks = htonl(_num_remote_ranks);
        struct in_addr ip;
        inet_aton(_connections[0].client->LocalIpAddress().c_str(), &ip);
        *info_id = HpcStream::HToNLL(((uint64_t)ip.s_addr << 32) + (uint64_t)_connections[0].client->LocalPort());
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
                        v.length = HpcStream::NToHLL(*((int64_t*)(data + vars_offset)));
                        vars_offset += sizeof(int64_t);
                        if (v.length == 0)
                        {
                            int j;
                            uint32_t len;
                            v.g_size = new uint32_t[v.dims];
                            v.l_size = new uint32_t[v.dims];
                            v.l_offset = new uint32_t[v.dims];
                            memset(v.g_size, 0, v.dims * sizeof(uint32_t));
                            memset(v.l_size, 0, v.dims * sizeof(uint32_t));
                            memset(v.l_offset, 0, v.dims * sizeof(uint32_t));
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
                            v.val = NULL;
                        }
                        else
                        {
                            v.val = new uint8_t[v.size];
                        }
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

void HpcStream::Client::Read()
{
    int i, j;
    int num_connections = _connections.size();
    bool *receive_data = new bool[num_connections];
    memset(receive_data, 0, num_connections * sizeof(bool));
    bool all_received = false;
    while (!all_received)
    {
        for (i = 0; i < num_connections; i++)
        {
            if (!receive_data[i])
            {
                NetSocket::Client::Event event = _connections[i].client->PollForNextEvent();
                if (event.type == NetSocket::Client::EventType::ReceiveBinary)
                {
                    if (event.data_length > 4) // variable value
                    {
                        uint32_t name_len = *((uint32_t*)event.binary_data);
                        std::string name = std::string((char*)event.binary_data + sizeof(uint32_t), name_len);
                        int offset = sizeof(uint32_t) + name_len;
                        memcpy(_connections[i].vars[name].val, (uint8_t*)event.binary_data + offset, event.data_length - offset);
                        // if array size, copy value to respective arrays
                        if (_connections[i].vars[name].dims == 1 && _connections[i].vars[name].length == 1 
                            && _connections[i].vars[name].type == HpcStream::DataType::ArraySize)
                        {
                            for (auto& x : _connections[i].vars)
                            {
                                uint32_t pos = find(x.second.gs_vars.begin(), x.second.gs_vars.end(), name) - x.second.gs_vars.begin();
                                if (pos < x.second.gs_vars.size())
                                {
                                    x.second.g_size[pos] = *((uint32_t*)_connections[i].vars[name].val);
                                }
                                pos = find(x.second.ls_vars.begin(), x.second.ls_vars.end(), name) - x.second.ls_vars.begin();
                                if (pos < x.second.ls_vars.size())
                                {
                                    x.second.l_size[pos] = *((uint32_t*)_connections[i].vars[name].val);
                                    // allocate local value array if all local sizes are non-zero
                                    bool non_zero = true;
                                    uint32_t length = 1;
                                    for (j = 0; j < x.second.dims; j++)
                                    {
                                        non_zero &= x.second.l_size[j] != 0;
                                        length *= x.second.l_size[j];
                                    }
                                    if (non_zero)
                                    {
                                        if (x.second.val != NULL) delete[] x.second.val;
                                        x.second.length = length;
                                        x.second.val = new uint8_t[x.second.size * x.second.length];
                                    }
                                }
                                pos = find(x.second.lo_vars.begin(), x.second.lo_vars.end(), name) - x.second.lo_vars.begin();
                                if (pos < x.second.lo_vars.size())
                                {
                                    x.second.l_offset[pos] = *((uint32_t*)_connections[i].vars[name].val);
                                }
                            }
                        }
                    }
                    else if (event.data_length == 1 && *((uint8_t*)event.binary_data) == 255) // end notification
                    {
                        receive_data[i] = true;
                    }
                    delete[] event.binary_data;
                }
            }
        }
        if (std::all_of(receive_data, receive_data + num_connections, [](bool x) {return x;}))
        {
            all_received = true;
        }
    }

    delete[] receive_data;
}

void HpcStream::Client::ReleaseTimeStep()
{
    int i;
    uint8_t complete = 255;
    for (i = 0; i < _connections.size(); i++)
    {
        _connections[i].client->Send(&complete, 1, NetSocket::CopyMode::MemCopy);
    }
}

void HpcStream::Client::GetGlobalSizeForVariable(std::string var_name, uint32_t *size)
{
    if (_connections[0].vars[var_name].gs_vars.size() == 0)
    {
        *size = 0;
    }
    else
    {
        int i;
        for (i = 0; i < _connections[0].vars[var_name].dims; i++)
        {
            size[i] = _connections[0].vars[var_name].g_size[i];
        }
    }
}

HpcStream::Client::GlobalSelection HpcStream::Client::CreateGlobalArraySelection(std::string var_name, int32_t *sizes, int32_t *offsets)
{
    GlobalSelection selection;
    selection.var_name = var_name;
    uint32_t dims = _connections[0].vars[var_name].dims;
    int problem_type;
    if (dims == 1)
    {
        problem_type = DDR_DATA_TYPE_CONTINUOUS;
    }
    else if (dims == 2)
    {
        problem_type = DDR_DATA_TYPE_REGULAR_GRID_2D;
    }
    else if (dims == 3)
    {
        problem_type = DDR_DATA_TYPE_REGULAR_GRID_3D;
    }
    else
    {
        fprintf(stderr, "[HpcStream] Error: currently only support 1D, 2D, and 3D arrays\n");
    }
    MPI_Datatype type;
    switch (_connections[0].vars[var_name].type)
    {
        case DataType::Int8:
            type = MPI_SIGNED_CHAR;
            break;
        case DataType::Uint8:
            type = MPI_UINT8_T;
            break;
        case DataType::Int16:
            type = MPI_SHORT;
            break;
        case DataType::Uint16:
            type = MPI_UINT16_T;
            break;
        case DataType::Int32:
            type = MPI_INT;
            break;
        case DataType::Uint32:
            type = MPI_UINT32_T;
            break;
        case DataType::Float:
            type = MPI_FLOAT;
            break;
        case DataType::ArraySize:
            type = MPI_UINT32_T;
            break;
        case DataType::Int64:
            type = MPI_LONG_LONG;
            break;
        case DataType::Uint64:
            type = MPI_UINT64_T;
            break;
        case DataType::Double:
            type = MPI_DOUBLE;
            break; 
    }
    selection.desc = DDR_NewDataDescriptor(_num_ranks, problem_type, type, HpcStream::GetDataTypeSize(_connections[0].vars[var_name].type));

    int i, j;
    int chunks_own = _connections.size();
    int *dims_own = new int[chunks_own * dims];
    int *offsets_own = new int[chunks_own * dims];
    for (i = 0; i < _connections.size(); i++)
    {
        printf("[rank %d] c %d: ", _rank, i);
        for (j = 0; j < dims; j++)
        {
            dims_own[i * dims + j] = _connections[i].vars[var_name].l_size[j];
            offsets_own[i * dims + j] = _connections[i].vars[var_name].l_offset[j];
            printf("d %d o %d (%d)  ", dims_own[i * dims + j], offsets_own[i * dims + j], i * dims + j);
        }
        printf("\n");
    }
    printf("[rank %d] want: sizes %dx%d, offsets %d %d\n", _rank, sizes[0], sizes[1], offsets[0], offsets[1]);

    DDR_SetupDataMapping(_rank, _num_ranks, chunks_own, dims_own, offsets_own, sizes, offsets, selection.desc);

    return selection;
}

void HpcStream::Client::FillSelection(GlobalSelection& selection, void *data)
{
    int i;
    uint32_t data_size = 0;
    for (i = 0; i < _connections.size(); i++)
    {
        data_size += _connections[i].vars[selection.var_name].length * _connections[i].vars[selection.var_name].size;
    }
    uint8_t *d_own = new uint8_t[data_size];
    uint32_t offset = 0;
    for (i = 0; i < _connections.size(); i++)
    {
        memcpy(d_own + offset, _connections[i].vars[selection.var_name].val, _connections[i].vars[selection.var_name].length * _connections[i].vars[selection.var_name].size);
        offset += _connections[i].vars[selection.var_name].length * _connections[i].vars[selection.var_name].size;
    }
    DDR_ReorganizeData(_num_ranks, d_own, data, selection.desc);
}
