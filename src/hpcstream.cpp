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
    uint64_t rval;
    uint8_t *data = (uint8_t *)&rval;

    data[0] = val >> 56;
    data[1] = val >> 48;
    data[2] = val >> 40;
    data[3] = val >> 32;
    data[4] = val >> 24;
    data[5] = val >> 16;
    data[6] = val >> 8;
    data[7] = val >> 0;

    return rval;
#endif
}

uint64_t HpcStream::NToHLL(uint64_t val)
{
#if __BYTE_ORDER == __BIG_ENDIAN
    return val;
#else
    uint64_t rval;
    uint8_t *data = (uint8_t *)&rval;

    data[0] = val >> 56;
    data[1] = val >> 48;
    data[2] = val >> 40;
    data[3] = val >> 32;
    data[4] = val >> 24;
    data[5] = val >> 16;
    data[6] = val >> 8;
    data[7] = val >> 0;

    return rval;
#endif
}

