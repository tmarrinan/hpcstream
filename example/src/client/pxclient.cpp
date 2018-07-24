#include <iostream>
#include <vector>
#include <numeric>
#include <mpi.h>
#include <ddr.h>
#include <glad/glad.h>]
#define GLFW_INCLUDE_NONE
#include <GLFW/glfw3.h>
#include <glm/mat4x4.hpp>
#include <glm/gtc/matrix_transform.hpp>
#include <glm/gtc/type_ptr.hpp>
#include "hpcstream/client.h"

typedef struct Screen {
    int width;
    int height;
    char title[96];
} Screen;
typedef struct GShaderProgram {
    GLuint program;
    GLint proj_uniform;
    GLint mv_uniform;
    GLint img_uniform;
} GShaderProgram;

void Init(GLFWwindow *window, Screen &screen, GShaderProgram *shader, GLuint *vao, GLuint *tex_id);
void Render(GLFWwindow *window, GShaderProgram& shader, GLuint vao, GLuint tex_id);
GLuint CreateRectangleVao();
GShaderProgram CreateTextureShader();
GLint CompileShader(char *source, uint32_t length, GLint type);
void CreateShaderProgram(GLint vertex_shader, GLint fragment_shader, GLuint *program);
void LinkShaderProgram(GLuint program);
int32_t ReadFile(const char* filename, char** data_ptr);

GLuint vertex_position_attrib = 0;
GLuint vertex_texcoord_attrib = 1;
glm::mat4 mat_projection;
glm::mat4 mat_modelview;

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

    // HpcStream clients
    if (argc < 3)
    {
        fprintf(stderr, "Error: no host and port provided for HpcStream server (rank 0)\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    HpcStream::Client stream(argv[1], atoi(argv[2]), MPI_COMM_WORLD);

    printf("[rank %d] CLIENT INITIALIZED\n", rank);





    int i;
    std::vector<NetSocket::Client*> clients;
    NetSocket::ClientOptions options = NetSocket::CreateClientOptions();
    options.secure = false;
    options.flags = NetSocket::GeneralFlags::None;
    int num_remote_ranks;
    uint8_t remote_endianness;
    uint8_t *remote_ip_addresses;
    uint16_t *remote_ports;
    if (rank == 0)
    {
        NetSocket::Client *c = new NetSocket::Client(argv[1], atoi(argv[2]), options);
        clients.push_back(c);
        int received_server_info = 0;
        while (received_server_info < 3)
        {
            NetSocket::Client::Event event = c->WaitForNextEvent();
            switch (event.type)
            {
                case NetSocket::Client::EventType::ReceiveBinary:
                    if (received_server_info == 0)      // endianness
                    {
                        remote_endianness = ((uint8_t*)event.binary_data)[0];
                        received_server_info++;
                    }
                    else if (received_server_info == 1) // ip addresses
                    {
                        num_remote_ranks = event.data_length / 4;
                        remote_ip_addresses = (uint8_t*)event.binary_data;
                        received_server_info++;
                    }
                    else                                // ports
                    {
                        remote_ports = (uint16_t*)event.binary_data;
                        for (i = 0; i<num_remote_ranks; i++)
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
    MPI_Bcast(&num_remote_ranks, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (rank != 0)
    {
        remote_ip_addresses = new uint8_t[4 * num_remote_ranks];
        remote_ports = new uint16_t[num_remote_ranks];
    }
    MPI_Bcast(remote_ip_addresses, 4 * num_remote_ranks, MPI_UNSIGNED_CHAR, 0, MPI_COMM_WORLD);
    MPI_Bcast(remote_ports, num_remote_ranks, MPI_UNSIGNED_SHORT, 0, MPI_COMM_WORLD);
    /*if (rank == 0)
    {
        uint8_t info_received[13];
        uint32_t *info_remote_ranks = (uint32_t*)info_received;
        uint32_t *info_rank = (uint32_t*)info_received + 1;
        uint32_t *info_ranks = (uint32_t*)info_received + 2;
        *info_remote_ranks = htonl(num_remote_ranks);
        *info_rank = htonl(rank);
        *info_ranks = htonl(num_ranks);
        info_received[12] = 0; // little endian
        clients[0]->Send(info_received, 13, NetSocket::CopyMode::MemCopy);
        if (clients[0]->WaitForNextEvent().type != NetSocket::Client::EventType::SendFinished)
        {
            printf("Error: unexpected event\n");
        }
    }*/
    int connections_per_rank = num_remote_ranks / num_ranks;
    int connections_extra = num_remote_ranks % num_ranks;
    int num_connections = connections_per_rank + (rank < connections_extra ? 1 : 0);
    int connection_offset = rank * connections_per_rank + std::min(rank, connections_extra);
    int *connection_widths = new int[num_connections];
    int *connection_heights = new int[num_connections];
    int *connection_offsets = new int[num_connections];
    int offset = 0;
    printf("connections: %d %d\n", connection_offset, num_connections);
    for (i = connection_offset; i < connection_offset + num_connections; i++)
    {
        NetSocket::Client *c;
        if (i > 0)
        {
            struct in_addr addr = {*((in_addr_t*)(&(remote_ip_addresses[4*i])))};
            printf("connecting to %s:%u\n", inet_ntoa(addr), remote_ports[i]);
            c = new NetSocket::Client(inet_ntoa(addr), remote_ports[i], options);
            NetSocket::Client::Event event;
            do
            {
                event = c->WaitForNextEvent();
            } while (event.type != NetSocket::Client::EventType::Connect);
            clients.push_back(c);
        }
        else
        {
            c = clients[0];
        }
    }

    uint8_t info_received[21];
    if (rank == 0)
    {
        uint32_t *info_remote_ranks = (uint32_t*)info_received;
        uint64_t *info_id = (uint64_t*)((uint32_t*)info_received + 1);
        uint32_t *info_ranks = (uint32_t*)info_received + 4;
        *info_remote_ranks = htonl(num_remote_ranks);
        *info_id = 0;
        struct in_addr ip;
        uint16_t port;
        inet_aton(clients[0]->IpAddress().c_str(), &ip);
        port = htons(clients[0]->Port());
        memcpy(info_id, &ip, 4);
        memcpy(info_id + 4, &port, 2);
        *info_ranks = htonl(num_ranks);
    }
    MPI_Bcast(info_received, 21, MPI_UINT8_T, 0, MPI_COMM_WORLD);
    uint32_t *info_rank = (uint32_t*)info_received + 3;
    *info_rank = htonl(rank);
    info_received[20] = 0; // little endian

    printf("[rank %d] num connections %d\n", rank, num_connections);
    for (i = 0; i < num_connections; i++)
    {
        clients[i]->Send(info_received, 21, NetSocket::CopyMode::MemCopy);
        if (clients[i]->WaitForNextEvent().type != NetSocket::Client::EventType::SendFinished)
        {
            printf("Error: unexpected event\n");
        }
    }

    for (i = connection_offset; i < connection_offset + num_connections; i++)
    {
        NetSocket::Client *c = clients[i - connection_offset];
        NetSocket::Client::Event event = c->WaitForNextEvent();
        if (event.type == NetSocket::Client::EventType::ReceiveBinary)
        {
            uint32_t vars_offset = 0;
            uint8_t* data = (uint8_t*)event.binary_data;
            while (vars_offset < event.data_length)
            {
                uint32_t var_name_len = ntohl(*((uint32_t*)(data + vars_offset)));
                vars_offset += sizeof(uint32_t);
                std::string var_name = std::string((char*)(data + vars_offset), var_name_len);
                vars_offset += var_name_len;
                uint32_t var_dims = ntohl(*((uint32_t*)(data + vars_offset)));
                vars_offset += sizeof(uint32_t);
                HpcStream::DataType var_type = (HpcStream::DataType)(*((uint8_t*)(data + vars_offset)));
                vars_offset += sizeof(uint8_t);
                uint32_t var_size = ntohl(*((uint32_t*)(data + vars_offset)));
                vars_offset += sizeof(uint32_t);
                int64_t var_length = ntohll(*((int64_t*)(data + vars_offset)));
                vars_offset += sizeof(int64_t);
                std::vector<std::string> gs_vars;
                std::vector<std::string> ls_vars;
                std::vector<std::string> lo_vars;
                if (var_length == 0)
                {
                    int j;
                    uint32_t len;
                    for (j = 0; j < var_dims; j++)
                    {
                        len = ntohl(*((uint32_t*)(data + vars_offset)));
                        vars_offset += sizeof(uint32_t);
                        std::string gs = std::string((char*)(data + vars_offset), len);
                        vars_offset += len;
                        gs_vars.push_back(gs);
                        printf("gs var: %s\n", gs.c_str());
                    }
                    for (j = 0; j < var_dims; j++)
                    {
                        len = ntohl(*((uint32_t*)(data + vars_offset)));
                        vars_offset += sizeof(uint32_t);
                        std::string ls = std::string((char*)(data + vars_offset), len);
                        vars_offset += len;
                        ls_vars.push_back(ls);
                        printf("ls var: %s\n", ls.c_str());
                    }
                    for (j = 0; j < var_dims; j++)
                    {
                        len = ntohl(*((uint32_t*)(data + vars_offset)));
                        vars_offset += sizeof(uint32_t);
                        std::string lo = std::string((char*)(data + vars_offset), len);
                        vars_offset += len;
                        lo_vars.push_back(lo);
                        printf("lo var: %s\n", lo.c_str());
                    }
                }
                //uint32_t *var_g_size = new uint32_t[var_dims * sizeof(uint32_t)];
                //memcpy(var_g_size, data + vars_offset, var_dims * sizeof(uint32_t));
                //vars_offset += var_dims * sizeof(uint32_t);
                //uint32_t *var_l_size = new uint32_t[var_dims * sizeof(uint32_t)];
                //memcpy(var_l_size, data + vars_offset, var_dims * sizeof(uint32_t));
                //vars_offset += var_dims * sizeof(uint32_t);
                //uint32_t *var_l_offset = new uint32_t[var_dims * sizeof(uint32_t)];
                //memcpy(var_l_offset, data + vars_offset, var_dims * sizeof(uint32_t));
                //vars_offset += var_dims * sizeof(uint32_t);
                printf("[rank %d] client %d: var = %s, %u (t=%u)\n", rank, i, var_name.c_str(), var_dims, var_type);
                /*if (var_name == "pixels")
                {
                    connection_widths[i - connection_offset] = 0;//var_l_size[0];
                    connection_heights[i - connection_offset] = 0;//var_l_size[1];
                    connection_offsets[i - connection_offset] = offset;
                    offset += connection_widths[i - connection_offset] * connection_heights[i - connection_offset];
                    printf("[rank %d] client %d: %dx%d\n", rank, i, connection_widths[i - connection_offset] / 4, connection_heights[i - connection_offset]);
                }*/
            }
        }
        else {
            printf("[rank %d] client %d: unexpected event %d\n", rank, i, event.type);
        }
    }


    // ...
    printf("Made it here\n");
    // ...


    // initialize GLFW
    if (!glfwInit())
    {
        exit(1);
    }

    // define screen properties
    Screen screen;
    screen.width = 1280;
    screen.height = 720;
    strcpy(screen.title, "PxStream Client");

    // create a window and its OpenGL context
    glfwWindowHint(GLFW_CONTEXT_VERSION_MAJOR, 3);
    glfwWindowHint(GLFW_CONTEXT_VERSION_MINOR, 2);
    glfwWindowHint(GLFW_OPENGL_FORWARD_COMPAT, GL_TRUE);
    glfwWindowHint(GLFW_OPENGL_PROFILE, GLFW_OPENGL_CORE_PROFILE);
    GLFWwindow *window = glfwCreateWindow(screen.width, screen.height, screen.title, NULL, NULL);

    // make window's context current
    glfwMakeContextCurrent(window);
    glfwSwapInterval(1);

    // initialize Glad OpenGL extension handling
    if (!gladLoadGLLoader((GLADloadproc)glfwGetProcAddress))
    {
        exit(1);
    }

    // main event loop
    GShaderProgram shader;
    GLuint vao;
    GLuint tex_id;
    Init(window, screen, &shader, &vao, &tex_id);

    bool *receive_data = new bool[num_connections];
    int tex_width;// = connection_widths[0] / 4;
    int tex_height;// = std::accumulate(connection_heights, connection_heights + num_connections, 0);
    //printf("texture: %dx%d\n", tex_width, tex_height);
    uint8_t *texture;// = new uint8_t[tex_width * tex_height * 4];
    uint8_t complete = 255;
    int it = 0;
    uint32_t local_size[2] = {0, 0};
    uint32_t local_offset[2] = {0, 0};
    while (!glfwWindowShouldClose(window))
    {
        glfwPollEvents();
        
        for (i = 0; i < num_connections; i++)
        {
            receive_data[i] = false;
        }
        bool all_received = false;
        printf("[rank %d] starting iteration %d\n", rank, it);
        while (!all_received)
        {
            for (i = 0; i < clients.size(); i++)
            {
                if (receive_data[i] == false)
                {
                    NetSocket::Client::Event event = clients[i]->PollForNextEvent();
                    if (event.type == NetSocket::Client::EventType::ReceiveBinary)
                    {
                        if (event.data_length > 4)
                        {
                            uint32_t name_len = *((uint32_t*)event.binary_data);
                            std::string name = std::string((char*)event.binary_data + sizeof(uint32_t), name_len);
                            if (name == "local_width")
                            {
                                local_size[0] = *((uint32_t*)((uint8_t*)event.binary_data + name_len + sizeof(uint32_t)));
                                printf("[rank %d] c %d: local_width %u\n", rank, i, local_size[0]);
                                if (local_size[1] != 0)
                                {
                                    connection_widths[i] = local_size[0];
                                    connection_heights[i] = local_size[1];
                                    if (local_offset[0] != 0 && local_offset[1] != 0)
                                    {
                                        connection_offsets[i] = connection_widths[i] * local_offset[1] + local_offset[0];
                                    }
                                    if (i == 0)
                                    {
                                        tex_width = connection_widths[i] / 4;
                                        tex_height = connection_heights[i];
                                        texture = new uint8_t[tex_width * tex_height * 4];
                                    }
                                    //connection_offsets[i - connection_offset] = offset;
                                    //offset += connection_widths[i] * connection_heights[i];
                                }
                            }
                            else if (name == "local_height")
                            {
                                local_size[1] = *((uint32_t*)((uint8_t*)event.binary_data + name_len + sizeof(uint32_t)));
                                printf("[rank %d] c %d: local_height %u\n", rank, i, local_size[1]);
                                if (local_size[0] != 0)
                                {
                                    connection_widths[i] = local_size[0];
                                    connection_heights[i] = local_size[1];
                                    if (local_offset[0] != 0 && local_offset[1] != 0)
                                    {
                                        connection_offsets[i] = connection_widths[i] * local_offset[1] + local_offset[0];
                                    }
                                    //connection_offsets[i - connection_offset] = offset;
                                    //offset += connection_widths[i] * connection_heights[i];
                                }
                            }
                            else if (name == "local_offsetx")
                            {
                                local_offset[0] = *((uint32_t*)((uint8_t*)event.binary_data + name_len + sizeof(uint32_t)));
                                printf("[rank %d] c %d: local_offsetx %u\n", rank, i, local_offset[0]);
                                if (local_offset[1] != 0 && connection_widths[i] != 0)
                                {
                                    connection_offsets[i] = connection_widths[i] * local_offset[1] + local_offset[0];
                                }
                            }
                            else if (name == "local_offsety")
                            {
                                local_offset[1] = *((uint32_t*)((uint8_t*)event.binary_data + name_len + sizeof(uint32_t)));
                                printf("[rank %d] c %d: local_offsety %u\n", rank, i, local_offset[1]);
                                if (local_offset[0] != 0 && connection_widths[i] != 0)
                                {
                                    connection_offsets[i] = connection_widths[i] * local_offset[1] + local_offset[0];
                                }
                            }
                            else if (name == "pixels")
                            {
                                printf("[rank %d] c %d: copying pixels to texture (%u)\n", rank, i, event.data_length - (name_len + sizeof(uint32_t)));
                                //memcpy(texture + connection_offsets[i], (uint8_t*)event.binary_data + name_len + sizeof(uint32_t), 
                                //       event.data_length - (name_len + sizeof(uint32_t)));
                                if (i == 0)
                                {
                                    memcpy(texture, (uint8_t*)event.binary_data + name_len + sizeof(uint32_t), 
                                           event.data_length - (name_len + sizeof(uint32_t)));
                                }
                                delete[] event.binary_data;
                            }
                            else if (name == "time_step")
                            {
                                uint32_t time_step = *((uint32_t*)((uint8_t*)event.binary_data + name_len + sizeof(uint32_t)));
                                printf("[rank %d] c %d: time step %u\n", rank, i, time_step);
                                delete[] event.binary_data;
                            }
                            else
                            {
                                printf("[rank %d] c %d: received %s\n", rank, i, name.c_str());
                            }
                        }
                        else if (event.data_length == 1 && *((uint8_t*)event.binary_data) == 255)
                        {
                            printf("[rank %d] c %d: all received\n", rank, i);
                            receive_data[i] = true;
                        }
                    }
                }
            }
            all_received = true;
            for (i = 0; i < num_connections; i++)
            {
                all_received &= receive_data[i];
            }
        }

        printf("[rank %d] sending complete time step\n", rank);
        for (i = 0; i < clients.size(); i++)
        {
            clients[i]->Send(&complete, 1, NetSocket::CopyMode::ZeroCopy);
        }
        printf("[rank %d] about to upload texture\n", rank);
        glBindTexture(GL_TEXTURE_2D, tex_id);
        glTexImage2D(GL_TEXTURE_2D, 0, GL_RGBA, tex_width, tex_height, 0, GL_RGBA, GL_UNSIGNED_BYTE, texture);
        glBindTexture(GL_TEXTURE_2D, 0);
        Render(window, shader, vao, tex_id);
        printf("[rank %d] showing iteration %d (%u %u %u %u)\n", rank, it, texture[0], texture[1], texture[2], texture[3]);

        /*for (i = 0; i < clients.size(); i++)
        {
            NetSocket::Client::Event event;
            do
            {
                event = clients[i]->PollForNextEvent();
            } while (event.type != NetSocket::Client::EventType::SendFinished);
        }*/
        printf("[rank %d] finishing iteration %d\n", rank, it);
        it++;
    }

    // finalize
    glfwDestroyWindow(window);
    glfwTerminate();
    MPI_Finalize();

    return 0;
}

void Init(GLFWwindow *window, Screen &screen, GShaderProgram *shader, GLuint *vao, GLuint *tex_id)
{
    int w, h;
    glClearColor(0.0, 0.0, 1.0, 1.0);
    glEnable(GL_DEPTH_TEST);
    glfwGetFramebufferSize(window, &w, &h);
    glViewport(0, 0, w, h);
    printf("OpenGL: %s\n", glGetString(GL_VERSION));

    *shader = CreateTextureShader();
    *vao = CreateRectangleVao();

    GLubyte blank[4] = {255, 255, 0, 255};
    glGenTextures(1, tex_id);
    glBindTexture(GL_TEXTURE_2D, *tex_id);
    glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MAG_FILTER, GL_LINEAR);
    glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MIN_FILTER, GL_LINEAR);
    glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_WRAP_S, GL_CLAMP_TO_EDGE);
    glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_WRAP_T, GL_CLAMP_TO_EDGE);
    glTexImage2D(GL_TEXTURE_2D, 0, GL_RGBA, 1, 1, 0, GL_RGBA, GL_UNSIGNED_BYTE, blank);
    glBindTexture(GL_TEXTURE_2D, 0);

    mat_projection = glm::ortho(0.0, (double)w, (double)h, 0.0, 1.0, -1.0);
    mat_modelview = glm::scale(glm::mat4(1.0), glm::vec3(w, h, 1.0));

    Render(window, *shader, *vao, *tex_id);
}

void Render(GLFWwindow *window, GShaderProgram& shader, GLuint vao, GLuint tex_id)
{
    glClear(GL_COLOR_BUFFER_BIT | GL_DEPTH_BUFFER_BIT);

    glUseProgram(shader.program);
    glUniformMatrix4fv(shader.proj_uniform, 1, GL_FALSE, glm::value_ptr(mat_projection));
    glActiveTexture(GL_TEXTURE0);
    glBindVertexArray(vao);
    glUniformMatrix4fv(shader.mv_uniform, 1, GL_FALSE, glm::value_ptr(mat_modelview));
    glBindTexture(GL_TEXTURE_2D, tex_id);
    glUniform1i(shader.img_uniform, 0);
    glDrawElements(GL_TRIANGLES, 6, GL_UNSIGNED_SHORT, 0);
    glBindTexture(GL_TEXTURE_2D, 0);
    glBindVertexArray(0);

    MPI_Barrier(MPI_COMM_WORLD);

    glfwSwapBuffers(window);
}

GLuint CreateRectangleVao()
{
    GLuint vao;
    glGenVertexArrays(1, &vao);
    glBindVertexArray(vao);

    // vertices
    GLuint vertex_position_buffer;
    glGenBuffers(1, &vertex_position_buffer);
    glBindBuffer(GL_ARRAY_BUFFER, vertex_position_buffer);
    GLfloat vertices[12] = {
        0.0, 0.0, 0.0,
        0.0, 1.0, 0.0,
        1.0, 0.0, 0.0,
        1.0, 1.0, 0.0
    };
    glBufferData(GL_ARRAY_BUFFER, 12 * sizeof(GLfloat), vertices, GL_STATIC_DRAW);
    glEnableVertexAttribArray(vertex_position_attrib);
    glVertexAttribPointer(vertex_position_attrib, 3, GL_FLOAT, false, 0, 0);

    // textures
    GLuint vertex_texcoord_buffer;
    glGenBuffers(1, &vertex_texcoord_buffer);
    glBindBuffer(GL_ARRAY_BUFFER, vertex_texcoord_buffer);
    GLfloat texcoords[8] = {
        0.0, 0.0,
        0.0, 1.0,
        1.0, 0.0,
        1.0, 1.0
    };
    glBufferData(GL_ARRAY_BUFFER, 8 * sizeof(GLfloat), texcoords, GL_STATIC_DRAW);
    glEnableVertexAttribArray(vertex_texcoord_attrib);
    glVertexAttribPointer(vertex_texcoord_attrib, 2, GL_FLOAT, false, 0, 0);

    // faces of the triangles
    GLuint vertex_index_buffer;
    glGenBuffers(1, &vertex_index_buffer);
    glBindBuffer(GL_ELEMENT_ARRAY_BUFFER, vertex_index_buffer);
    GLushort indices[6] = {
        0, 3, 1,
        3, 0, 2
    };
    glBufferData(GL_ELEMENT_ARRAY_BUFFER, 6 * sizeof(GLushort), indices, GL_STATIC_DRAW);

    glBindVertexArray(0);

    return vao;
}

GShaderProgram CreateTextureShader()
{
    GShaderProgram shader;

    const char *vertex_file = "example/resrc/shaders/texture.vert";
    char *vertex_src;
    int32_t vertex_src_length = ReadFile(vertex_file, &vertex_src);
    GLint vertex_shader = CompileShader(vertex_src, vertex_src_length, GL_VERTEX_SHADER);
    free(vertex_src);

    const char *fragment_file = "example/resrc/shaders/texture.frag";
    char *fragment_src;
    int32_t fragment_src_length = ReadFile(fragment_file, &fragment_src);
    GLint fragment_shader = CompileShader(fragment_src, fragment_src_length, GL_FRAGMENT_SHADER);
    free(fragment_src);

    CreateShaderProgram(vertex_shader, fragment_shader, &shader.program);

    glBindAttribLocation(shader.program, vertex_position_attrib, "aVertexPosition");
    glBindAttribLocation(shader.program, vertex_texcoord_attrib, "aVertexTexCoord");
    glBindAttribLocation(shader.program, 0, "FragColor");

    LinkShaderProgram(shader.program);

    shader.mv_uniform = glGetUniformLocation(shader.program, "uModelViewMatrix");
    shader.proj_uniform = glGetUniformLocation(shader.program, "uProjectionMatrix");
    shader.img_uniform = glGetUniformLocation(shader.program, "uImage");

    return shader;
}

GLint CompileShader(char *source, uint32_t length, GLint type)
{
    GLint status;
    GLint shader = glCreateShader(type);

    const char *src_bytes = source;
    const GLint len = length;
    glShaderSource(shader, 1, &src_bytes, &len);
    glCompileShader(shader);
    glGetShaderiv(shader, GL_COMPILE_STATUS, &status);
    if (status == 0)
    {
        GLint log_length;
        glGetShaderiv(shader, GL_INFO_LOG_LENGTH, &log_length);
        char *info = (char*)malloc(log_length + 1);
        glGetShaderInfoLog(shader, log_length, NULL, info);
        fprintf(stderr, "Error: failed to compile shader:\n%s\n", info);
        free(info);

        return -1;
    }

    return shader;
}

void CreateShaderProgram(GLint vertex_shader, GLint fragment_shader, GLuint *program)
{
    *program = glCreateProgram();
    glAttachShader(*program, vertex_shader);
    glAttachShader(*program, fragment_shader);
}

void LinkShaderProgram(GLuint program)
{
    GLint status;
    glLinkProgram(program);

    glGetProgramiv(program, GL_LINK_STATUS, &status);
    if (status == 0)
    {
        fprintf(stderr, "Error: unable to initialize shader program\n");
    }
}

int32_t ReadFile(const char* filename, char** data_ptr)
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

    *data_ptr = (char*)malloc(fsize);
    size_t read = fread(*data_ptr, fsize, 1, fp);
    if (read != 1)
    {
        fprintf(stderr, "Error cannot read %s\n", filename);
        return -1;
    }

    fclose(fp);

    return fsize;
}
