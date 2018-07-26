#include "hpcstream.h"

uint32_t HpcStream::GetDataTypeSize(DataType type)
{
    uint32_t size = 0;
    switch (type)
    {
        case DataType::Int8:
        case DataType::Uint8:
            size = 1;
            break;
        case DataType::Int16:
        case DataType::Uint16:
            size = 2;
            break;
        case DataType::Int32:
        case DataType::Uint32:
        case DataType::Float:
        case DataType::ArraySize:
            size = 4;
            break;
        case DataType::Int64:
        case DataType::Uint64:
        case DataType::Double:
            size = 8;
            break;
    }
    return size;
}

uint64_t HpcStream::HToNLL(uint64_t val)
{
#if __BYTE_ORDER == __BIG_ENDIAN
    return val; 
#else
    return (((uint64_t)htonl(val)) << 32) + (uint64_t)htonl(val >> 32);
#endif
}

uint64_t HpcStream::NToHLL(uint64_t val)
{
#if __BYTE_ORDER == __BIG_ENDIAN
    return val;
#else
    return (((uint64_t)ntohl(val) >> 32) + (uint64_t)ntohl(val) << 32);
#endif
}

