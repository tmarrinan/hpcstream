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