#include "hpcstream/server.h"

HpcStream::Server::Server(const char *iface, uint16_t port_min, uint16_t port_max, MPI_Comm comm) :
    _num_connections(0),
    _ip_address_list(NULL),
    _port_list(NULL),
    _server(NULL)
{
    MPI_Comm_dup(comm, &_comm);
    int rc = MPI_Comm_rank(_comm, &_rank);
    rc |= MPI_Comm_size(_comm, &_num_ranks);
    if (rc != 0) {
        fprintf(stderr, "Error obtaining MPI task ID information\n");
    }

    NetSocket::ServerOptions options = NetSocket::CreateServerOptions();
    int i;
    int num_ports = port_max - port_min + 1;
    uint16_t *port_options = new uint16_t[num_ports];
    for (i = 0; i < num_ports; i++)
    {
        port_options[i] = port_min + i;
    }
    std::shuffle(port_options, port_options + num_ports, std::default_random_engine(time(0)));
    i = 0;
    _port = port_options[0];
    bool address_in_use = true;
    while (address_in_use && _port <= port_max)
    {
        try
        {
            _server = new NetSocket::Server(_port, options);
            address_in_use = false;
        }
        catch (std::exception& e)
        {
            i++;
            _port = port_options[i];
        }
    }
    delete[] port_options;

    uint8_t ip_address[4];
    GetIpAddress(iface, ip_address);
    uint16_t net_port = htons(_port);
    if (_rank == 0)
    {
        _ip_address_list = new uint8_t[4 * _num_ranks];
        _port_list = new uint16_t[_num_ranks];
    }
    MPI_Gather(ip_address, 4, MPI_UINT8_T, _ip_address_list, 4, MPI_UINT8_T, 0, _comm);
    MPI_Gather(&net_port, 1, MPI_UINT16_T, _port_list, 1, MPI_UINT16_T, 0, _comm);

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
}

HpcStream::Server::~Server()
{
    // TODO: stop server
}

char* HpcStream::Server::GetMasterIpAddress()
{
    char *addr;
    if (_rank == 0)
    {
        struct in_addr ip = {*((in_addr_t*)(_ip_address_list))};
        addr = inet_ntoa(ip);
    }
    else
    {
        addr = "";
    }
    return addr;
}

uint16_t HpcStream::Server::GetMasterPort()
{
    uint16_t port;
    if (_rank == 0)
    {
        port = ntohs(_port_list[0]);
    }
    else
    {
        port = 0;
    }
    return port;
}

void HpcStream::Server::DefineVar(std::string name, HpcStream::DataType base_type, std::string global_size, std::string local_size, std::string local_offset)
{
    int i;
    SharedVar var;
    var.type = base_type;
    int dim_a = std::count(global_size.begin(), global_size.end(), ',') + 1;
    int dim_b = std::count(local_size.begin(), local_size.end(), ',') + 1;
    int dim_c = std::count(local_offset.begin(), local_offset.end(), ',') + 1;
    if (dim_a != dim_b || dim_a != dim_c ||
        !((global_size.empty() && local_size.empty() && local_offset.empty()) || 
        (!global_size.empty() && !local_size.empty() && !local_offset.empty())))
    {
        fprintf(stderr, "[HpcStream] Error: global and local dimensions do not match (%s)\n", name.c_str());
        return;
    }
    var.dims = dim_a;
    var.gs_vars = ParseVarCounts(global_size);
    var.ls_vars = ParseVarCounts(local_size);
    var.lo_vars = ParseVarCounts(local_offset);
    switch (base_type)
    {
        case DataType::Int8:
        case DataType::Uint8:
            var.size = 1;
            break;
        case DataType::Int16:
        case DataType::Uint16:
            var.size = 2;
            break;
        case DataType::Int32:
        case DataType::Uint32:
        case DataType::Float:
        case DataType::ArraySize:
            var.size = 4;
            break;
        case DataType::Int64:
        case DataType::Uint64:
        case DataType::Double:
            var.size = 8;
            break; 
    }
    if (var.gs_vars.size() == 0)
    {
        var.length = 1;
        var.val = new uint8_t[var.size];
        memset(var.val, 0, var.size);
        var.g_size = NULL;
        var.l_size = NULL;
        var.l_offset = NULL;
    }
    else
    {
        var.length = 0;
        var.val = NULL;
        var.g_size = new uint32_t[var.dims];
        var.l_size = new uint32_t[var.dims];
        var.l_offset = new uint32_t[var.dims];
        memset(var.g_size, 0, var.dims * sizeof(uint32_t));
        memset(var.l_size, 0, var.dims * sizeof(uint32_t));
        memset(var.l_offset, 0, var.dims * sizeof(uint32_t));
    }
    var.updated = false;

    _vars[name] = var;
}

void HpcStream::Server::VarDefinitionsComplete(StreamBehavior behavior, int initial_wait_count)
{
    _stream_behavior = behavior;
    GenerateVarsBuffer();

    while (_num_connections < initial_wait_count)
    {
        NetSocket::Server::Event event = _server->WaitForNextEvent();
        if (!HandleNewConnection(event))
        {
            if (event.type != NetSocket::Server::EventType::SendFinished)
            {
                if (event.type == NetSocket::Server::EventType::ReceiveBinary)
                {
                    delete[] event.binary_data;
                }
                fprintf(stderr, "unexpected event while connecting to clients\n");
            }
        }
    }
}

void HpcStream::Server::SetValue(std::string name, void *value)
{
    if (_vars[name].length == 0)
    {
        fprintf(stderr, "[HpcStream] Error: cannot set value without initializing sizes\n");
        return;
    }
    if (_vars[name].gs_vars.size() == 0) // non-array
    {
        memcpy(_vars[name].val, value, _vars[name].size * _vars[name].length);
    }
    else
    {
        _vars[name].val = (uint8_t*)value;
    }
    if (_vars[name].dims == 1 && _vars[name].length == 1 && _vars[name].type == HpcStream::DataType::ArraySize)
    {
        int i;
        for (auto& x : _vars)
        {
            uint32_t pos = find(x.second.gs_vars.begin(), x.second.gs_vars.end(), name) - x.second.gs_vars.begin();
            if (pos < x.second.gs_vars.size())
            {
                x.second.g_size[pos] = *((uint32_t*)_vars[name].val);
            }
            pos = find(x.second.ls_vars.begin(), x.second.ls_vars.end(), name) - x.second.ls_vars.begin();
            if (pos < x.second.ls_vars.size())
            {
                x.second.l_size[pos] = *((uint32_t*)_vars[name].val);
                bool non_zero = true;
                uint32_t length = 1;
                for (i = 0; i < x.second.dims; i++)
                {
                    non_zero &= x.second.l_size[i] != 0;
                    length *= x.second.l_size[i];
                }
                if (non_zero)
                {
                    //if (x.second.val != NULL) delete[] x.second.val;
                    x.second.length = length;
                    //x.second.val = new uint8_t[x.second.size * x.second.length];
                }
            }
            pos = find(x.second.lo_vars.begin(), x.second.lo_vars.end(), name) - x.second.lo_vars.begin();
            if (pos < x.second.lo_vars.size())
            {
                x.second.l_offset[pos] = *((uint32_t*)_vars[name].val);
            }
        }
    }
    _vars[name].updated = true;
}

void HpcStream::Server::Write()
{
    bool new_conn = false;
    for (auto const& c : _connections)
    {
        new_conn |= c.second.is_new;
    }
    for (auto& x : _vars)
    {
        if ((x.second.updated || new_conn) && x.second.gs_vars.size() == 0)
        {
            uint32_t name_len = x.first.length();
            uint32_t send_size = sizeof(uint32_t) + name_len + (x.second.size * x.second.length);
            uint8_t *send_buffer = new uint8_t[send_size];
            memcpy(send_buffer, &name_len, sizeof(uint32_t));
            memcpy(send_buffer + sizeof(uint32_t), x.first.c_str(), name_len);
            memcpy(send_buffer + sizeof(uint32_t) + name_len, x.second.val, x.second.size * x.second.length);
            for (auto const& c : _connections)
            {
                if (c.second.is_new || x.second.updated)
                {
                    c.second.client->Send(send_buffer, send_size, NetSocket::CopyMode::MemCopy);
                }
            }
            delete[] send_buffer;
            x.second.updated = false;
        }
    }
    for (auto& x : _vars)
    {
        if ((x.second.updated || new_conn) && x.second.gs_vars.size() > 0)
        {
            uint32_t name_len = x.first.length();
            uint32_t send_size = sizeof(uint32_t) + name_len + (x.second.size * x.second.length);
            uint8_t *send_buffer = new uint8_t[send_size];
            memcpy(send_buffer, &name_len, sizeof(uint32_t));
            memcpy(send_buffer + sizeof(uint32_t), x.first.c_str(), name_len);
            memcpy(send_buffer + sizeof(uint32_t) + name_len, x.second.val, x.second.size * x.second.length);
            for (auto const& c : _connections)
            {
                if (c.second.is_new || x.second.updated)
                {
                    //c.second.client->Send(send_buffer, send_size, NetSocket::CopyMode::MemCopy);
                    c.second.client->Send(send_buffer, send_size, NetSocket::CopyMode::ZeroCopy);
                }
            }
            delete[] send_buffer;
            x.second.updated = false;
        }
    }
    uint8_t done = 255;
    for (auto& c : _connections)
    {
        c.second.client->Send(&done, 1, NetSocket::CopyMode::MemCopy);
        if (c.second.is_new)
        {
            c.second.is_new = false;
        }
    }
}

void HpcStream::Server::AdvanceTimeStep()
{
    if (_stream_behavior == StreamBehavior::WaitForAll)
    {
        bool all_ready_to_advance = false;
        std::map<std::string, bool> client_ready_to_advance;
        for (auto const& c : _connections)
        {
            client_ready_to_advance[c.first] = false;
        }
        std::string event_client_id;
        while (!all_ready_to_advance)
        {
            NetSocket::Server::Event event = _server->WaitForNextEvent();
            if (event.type != NetSocket::Server::EventType::None)
            {
                event_client_id = event.client->Endpoint();
            }
            if (!HandleNewConnection(event))
            {
                switch (event.type)
                {
                    case NetSocket::Server::EventType::ReceiveBinary:
                        if (client_ready_to_advance.find(event_client_id) != client_ready_to_advance.end()
                            && !client_ready_to_advance[event_client_id]
                            && _connections[event_client_id].state == ClientState::Streaming
                            && event.data_length == 1
                            && reinterpret_cast<uint8_t*>(event.binary_data)[0] == 255)
                        {
                            client_ready_to_advance[event_client_id] = true;
                            all_ready_to_advance = std::all_of(client_ready_to_advance.begin(), client_ready_to_advance.end(),
                                                               [](std::pair<std::string, bool> p) {return p.second;});
                        }
                        delete[] event.binary_data;
                        break;
                    default:
                        break;
                }
            }
        }
    }
    else {
        NetSocket::Server::Event event = _server->PollForNextEvent();
        while (event.type != NetSocket::Server::EventType::None)
        {
            if (!HandleNewConnection(event))
            {
                switch (event.type)
                {
                    case NetSocket::Server::EventType::ReceiveBinary:
                        delete[] event.binary_data;
                        break;
                    default:
                        break;
                }
            }
            event = _server->PollForNextEvent();
        }
    }
}

void HpcStream::Server::GenerateVarsBuffer()
{
    int i;
    _vars_buffer_size = 0;
    for (auto const& x : _vars)
    {
        _vars_buffer_size += x.first.length();
        _vars_buffer_size += sizeof(DataType) + 3 * sizeof(uint32_t) + sizeof(int64_t);
        if (x.second.length == 0)
        {
            for (i = 0; i < x.second.dims; i++)
            {
                _vars_buffer_size += 3 * sizeof(uint32_t);
                _vars_buffer_size += x.second.gs_vars[i].length();
                _vars_buffer_size += x.second.ls_vars[i].length();
                _vars_buffer_size += x.second.lo_vars[i].length();
            }
        }
    }
    _vars_buffer = new uint8_t[_vars_buffer_size];
    uint32_t vars_offset = 0;
    for (auto const& x : _vars)
    {
        uint32_t name_len = htonl(static_cast<uint32_t>(x.first.length()));
        memcpy(_vars_buffer + vars_offset, &name_len, sizeof(uint32_t));
        vars_offset += sizeof(uint32_t);
        memcpy(_vars_buffer + vars_offset, x.first.c_str(), x.first.length());
        vars_offset += x.first.length();
        uint32_t net_dims = htonl(x.second.dims);
        memcpy(_vars_buffer + vars_offset, &net_dims, sizeof(uint32_t));
        vars_offset += sizeof(uint32_t);
        memcpy(_vars_buffer + vars_offset, &x.second.type, sizeof(uint8_t));
        vars_offset += sizeof(uint8_t);
        uint32_t net_size = htonl(x.second.size);
        memcpy(_vars_buffer + vars_offset, &net_size, sizeof(uint32_t));
        vars_offset += sizeof(uint32_t);
        int64_t net_length = HpcStream::HToNLL(x.second.length);
        memcpy(_vars_buffer + vars_offset, &net_length, sizeof(int64_t));
        vars_offset += sizeof(int64_t);
        if (x.second.length == 0)
        {
            uint32_t len;
            for (i = 0; i < x.second.dims; i++)
            {
                len = htonl(static_cast<uint32_t>(x.second.gs_vars[i].length()));
                memcpy(_vars_buffer + vars_offset, &len, sizeof(uint32_t));
                vars_offset += sizeof(uint32_t);
                memcpy(_vars_buffer + vars_offset, x.second.gs_vars[i].c_str(), x.second.gs_vars[i].length());
                vars_offset += x.second.gs_vars[i].length();
            }
            for (i = 0; i < x.second.dims; i++)
            {
                len = htonl(static_cast<uint32_t>(x.second.ls_vars[i].length()));
                memcpy(_vars_buffer + vars_offset, &len, sizeof(uint32_t));
                vars_offset += sizeof(uint32_t);
                memcpy(_vars_buffer + vars_offset, x.second.ls_vars[i].c_str(), x.second.ls_vars[i].length());
                vars_offset += x.second.ls_vars[i].length();
            }
            for (i = 0; i < x.second.dims; i++)
            {
                len = htonl(static_cast<uint32_t>(x.second.lo_vars[i].length()));
                memcpy(_vars_buffer + vars_offset, &len, sizeof(uint32_t));
                vars_offset += sizeof(uint32_t);
                memcpy(_vars_buffer + vars_offset, x.second.lo_vars[i].c_str(), x.second.lo_vars[i].length());
                vars_offset += x.second.lo_vars[i].length();
            }
        }
    }
}

bool HpcStream::Server::HandleNewConnection(NetSocket::Server::Event& event)
{
    bool new_connection_event = false;
    std::string event_client_id;
    if (event.type != NetSocket::Server::EventType::None)
    {
        event_client_id = event.client->Endpoint();
    }
    switch (event.type)
    {
        case NetSocket::Server::EventType::Connect:
            _connections[event_client_id] = {0, ClientState::Connecting, event.client, 0, 0, true, false};
            if (_rank == 0)
            {
                // send server ip addresses and ports for all ranks
                _connections[event_client_id].client->Send(&_endianness, 1, NetSocket::CopyMode::ZeroCopy);
                _connections[event_client_id].client->Send(_ip_address_list, 4 * _num_ranks, NetSocket::CopyMode::ZeroCopy);
                _connections[event_client_id].client->Send(_port_list, _num_ranks * sizeof(uint16_t), NetSocket::CopyMode::ZeroCopy);
            }
            new_connection_event = true;
            break;
        case NetSocket::Server::EventType::Disconnect:
            // TODO: handle disconnect
            new_connection_event = true;
            break;
        case NetSocket::Server::EventType::ReceiveString:
            fprintf(stderr, "[HpcStream] Error: received unknown string message from %s\n", event_client_id.c_str());
            new_connection_event = true;
            break;
        case NetSocket::Server::EventType::ReceiveBinary:
            if (_connections[event_client_id].state == ClientState::Connecting)
            {
                _connections[event_client_id].state = ClientState::Handshake;
                 //verify client handshake data is as expected
                if (event.data_length == 21 && ntohl(*((uint32_t*)event.binary_data)) == _num_ranks)
                {
                    // store client data
                    _connections[event_client_id].id = HpcStream::NToHLL(*((uint64_t*)((uint32_t*)event.binary_data + 1)));
                    _connections[event_client_id].remote_rank = ntohl(*((uint32_t*)event.binary_data + 3));
                    _connections[event_client_id].num_remote_ranks = ntohl(*((uint32_t*)event.binary_data + 4));
                    _connections[event_client_id].has_same_endianness = ((uint8_t*)event.binary_data)[20] == _endianness;
                    // send variable definitions
                    _connections[event_client_id].client->Send(_vars_buffer, _vars_buffer_size, NetSocket::CopyMode::ZeroCopy);
                }
                else
                {
                    // TODO: terminate client connection
                }
                delete[] event.binary_data;
                new_connection_event = true;
            }
            break;
        case NetSocket::Server::EventType::SendFinished:
            // once variable definitions are sent, increment verified connections 
            if (event.binary_data == _vars_buffer)
            {
                _connections[event_client_id].state = ClientState::Streaming;
                printf("[rank %d] client %d (%s) connected and verified\n", _rank, _num_connections, event_client_id.c_str());
                _num_connections++;
                new_connection_event = true;
            }
            break;
        default:
            break;
    }
    return new_connection_event;
}

void HpcStream::Server::GetIpAddress(const char *iface, uint8_t ip_address[4])
{
    struct ifaddrs *interfaces = NULL;
    int success = getifaddrs(&interfaces);
    if (success == 0)
    {
        struct ifaddrs *temp_addr = interfaces;
        while (temp_addr != NULL)
        {
            if(temp_addr->ifa_addr->sa_family == AF_INET)
            {
                if (strcmp(temp_addr->ifa_name, iface) == 0)
                {
                    memcpy(ip_address, &(((struct sockaddr_in*)temp_addr->ifa_addr)->sin_addr), 4);
                    break;
                }
            }
            temp_addr = temp_addr->ifa_next;
        }
    }
    freeifaddrs(interfaces);
}

std::vector<std::string> HpcStream::Server::ParseVarCounts(std::string counts)
{
    std::vector<std::string> count_vars;
    std::istringstream iss(counts);
    std::string token;
    while (std::getline(iss, token, ','))
    {
        count_vars.push_back(std::string(token));
    }
    return count_vars;
}
