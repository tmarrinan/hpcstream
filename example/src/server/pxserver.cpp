#include <iostream>
#include <string>
#include <vector>
#include <netsocket/server.h>
#include <ifaddrs.h>
#include <mpi.h>
#include "hpcstream/server.h"
#define STB_IMAGE_IMPLEMENTATION
#include "stb_image.h"

typedef struct CompressedImageData {
    unsigned char *data;
    int32_t length;
} CompressedImageData;

typedef struct ImageData {
    unsigned char *data;
    int32_t width;
    int32_t height;
} ImageData;

void GetFrameList(int rank, std::string img_template, int start, int increment, std::vector<std::string> *frame_list);
void GetClosestFactors2(int value, int *factor_1, int *factor_2);
int32_t ReadFile(const char *filename, char **data);

int main(int argc, char **argv)
{
    // initialize MPI
    int rc, rank, num_ranks;
    rc = MPI_Init(&argc, &argv);
    rc |= MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    rc |= MPI_Comm_size(MPI_COMM_WORLD, &num_ranks);
    if (rc != 0)
    {
        fprintf(stderr, "Error initializing MPI and obtaining task ID information\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // find image sequence
    if (argc < 2)
    {
        fprintf(stderr, "Error: no input image sequence template provided\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    int start = 0;
    int increment = 1;
    if (argc >= 3) start = atoi(argv[2]);
    if (argc >= 4) increment = atoi(argv[3]);
    std::vector<std::string> frame_list;
    GetFrameList(rank, argv[1], start, increment, &frame_list);
    if (rank == 0) printf("Found %d images to stream\n", frame_list.size());

    // split into grid
    int rows;
    int cols;
    GetClosestFactors2(num_ranks, &cols, &rows);
    uint32_t m_col = rank % cols;
    uint32_t m_row = rank / cols;


    // read images into memory
    int i;
    char *img_data;
    int32_t img_len;
    std::vector<CompressedImageData> compressed_images;
    for (i = 0; i < frame_list.size(); i++)
    {
        img_len = ReadFile(frame_list[i].c_str(), &img_data);
        compressed_images.push_back({reinterpret_cast<unsigned char*>(img_data), img_len});
    }

    // decompress first image
    int w, h, c;
    ImageData send_img;
    send_img.data = stbi_load_from_memory(compressed_images[0].data, compressed_images[0].length, &w, &h, &c, STBI_rgb_alpha);
    send_img.width = w;
    send_img.height = h;
    if (rank == 0) printf("Image load complete (%dx%d)\n", w, h);

    // initialize HpcStream
    uint32_t global_width = w * cols;
    uint32_t global_height = h * rows;


    uint16_t port = 8000 + rank;
    uint32_t global_dim[2] = {global_width * 4, global_height};
    uint32_t local_dim[2] = {static_cast<uint32_t>(w) * 4, static_cast<uint32_t>(h)};
    uint32_t local_offset[2] = {m_col * w * 4, m_row * h};
    HpcStream::Server stream("lo0", port, MPI_COMM_WORLD);
    stream.DefineVar("time_step",     HpcStream::DataType::Uint32,    "", "", "");
    stream.DefineVar("global_width",  HpcStream::DataType::ArraySize, "", "", "");
    stream.DefineVar("global_height", HpcStream::DataType::ArraySize, "", "", "");
    stream.DefineVar("local_width",   HpcStream::DataType::ArraySize, "", "", "");
    stream.DefineVar("local_height",  HpcStream::DataType::ArraySize, "", "", "");
    stream.DefineVar("local_offsetx", HpcStream::DataType::ArraySize, "", "", "");
    stream.DefineVar("local_offsety", HpcStream::DataType::ArraySize, "", "", "");
    stream.DefineVar("pixels",        HpcStream::DataType::Uint8,     "global_width,global_height", "local_width,local_height", "local_offsetx,local_offsety"); 
    stream.VarDefinitionsComplete(HpcStream::Server::StreamBehavior::WaitForAll, 1);

    stream.SetValue("global_width", &(global_dim[0]));
    stream.SetValue("global_height", &(global_dim[1]));
    stream.SetValue("local_width", &(local_dim[0]));
    stream.SetValue("local_height", &(local_dim[1]));
    stream.SetValue("local_offsetx", &(local_offset[0]));
    stream.SetValue("local_offsety", &(local_offset[1]));

    // stream loop
    if (rank == 0) printf("Begin stream loop\n");
    for (i = 0; i <frame_list.size(); i++)
    {
        stream.SetValue("time_step", &i);
        stream.SetValue("pixels", send_img.data);
        stream.Write();

        send_img.data = stbi_load_from_memory(compressed_images[i].data, compressed_images[i].length, &w, &h, &c, STBI_rgb_alpha);
        send_img.width = w;
        send_img.height = h;

        stream.AdvanceTimeStep();
    }
    if (rank == 0) printf("all done - goodbye\n");
    // TODO: stream.Finalize()

    MPI_Finalize();
    
    return 0;
}

void GetFrameList(int rank, std::string img_template, int start, int increment, std::vector<std::string> *frame_list)
{
    std::string img_dir, img_seq, img_base, img_end, img_ext, frame_idx, frame_str, next_img, tmp;
    int i, sep, pos, padding;
    struct stat info;

    sep = img_template.rfind('/');
    if (sep == std::string::npos)
    {
        img_dir = "./";
        img_seq = img_template;
    }
    else
    {
        img_dir = img_template.substr(0, sep + 1);
        img_seq = img_template.substr(sep + 1);
    }
    if (stat(img_dir.c_str(), &info) != 0 || !(info.st_mode & S_IFDIR))
    {
        fprintf(stderr, "Error: %s is not a directory\n", img_dir.c_str());
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    pos = img_seq.find("%d");
    if (pos != std::string::npos)
    {
        padding = 0;
    }
    else
    {
        pos = img_seq.find("%0");
        if (pos != std::string::npos)
        {
            padding = atoi(img_seq.substr(pos + 1).c_str());
        }
        else
        {
            fprintf(stderr, "Error: must specify image sequence template (%%d or %%0Nd)\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    img_base = img_seq.substr(0, pos);
    tmp = img_seq.substr(pos);
    pos = tmp.find('d');
    if (pos == std::string::npos)
    {
        fprintf(stderr, "Error: must specify image sequence template (%%d or %%0Nd)\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    img_end = tmp.substr(pos + 1);
    pos = img_end.rfind(".");
    if (pos != std::string::npos)
    {
        img_ext = img_end.substr(pos);
        img_end = img_end.substr(0, pos);
    }
    else
    {
        fprintf(stderr, "Error: no image extension provided\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    i = 0;
    do
    {
        frame_idx = std::to_string(start + (i * increment));
        if (padding > 0)
        {
            tmp = std::string(padding, '0').append(frame_idx);
            frame_str = tmp.substr(tmp.length() - padding);
        }
        else
        {
            frame_str = frame_idx;
        }
        next_img = img_dir + img_base + frame_str + img_end + "_" + std::to_string(rank) + img_ext;
        frame_list->push_back(next_img);
        i++;
    } while (stat(next_img.c_str(), &info) == 0 && !(info.st_mode & S_IFDIR));
    frame_list->pop_back();
}

void GetClosestFactors2(int value, int *factor_1, int *factor_2)
{
    int test_num = (int)sqrt(value);
    while (value % test_num != 0)
    {
        test_num--;
    }
    *factor_2 = test_num;
    *factor_1 = value / test_num;
}

int32_t ReadFile(const char *filename, char **data)
{
    FILE *fp = fopen(filename, "rb");
    if (fp == NULL)
    {
        fprintf(stderr, "Error: cannot open %s\n", filename);
        return -1;
    }

    fseek(fp, 0, SEEK_END);
    int32_t fsize = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    *data = (char*)malloc(fsize);
    size_t read = fread(*data, fsize, 1, fp);
    if (read != 1)
    {
        fprintf(stderr, "Error: cannot read %s\n", filename);
        return -1;
    }
    fclose(fp);

    return fsize;
}
