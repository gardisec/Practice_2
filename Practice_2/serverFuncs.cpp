#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>//sleep func
#include <future>
#include <mutex>
#include <thread>
#include <arpa/inet.h>

#include "Head.h"

using namespace std;

void requestProcess(const int clientSocket, const sockaddr_in& clientAddress, const nlohmann::json& structure) {
    mutex userMutex;
    char receive[1024] = {};
    string sending;
    bool isExit = false;
    while (!isExit) {
        lock_guard<mutex> guard(userMutex);
        bzero(receive, 1024);
        const ssize_t userRead = read(clientSocket, receive, 1024);
        if (userRead <= 0) {
            cerr << "client " << clientAddress.sin_addr.s_addr << " disconnected\n";
            isExit = true;
            continue;
        }
        if (receive == "disconnect") {
            isExit = true;
            continue;
        }
        string result = request(receive);
        send(clientSocket, result.c_str(), result.size(), 0);
    }
    close(clientSocket);
}


void startServer() {
    json structureJSON;
    const int server = socket(AF_INET, SOCK_STREAM, 0);
    if (server == -1) {
        cerr << "Socket creation error" << endl;
        return;
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr("127.0.0.1");
    address.sin_port = htons(7432);

    if (bind(server, reinterpret_cast<struct sockaddr *>(&address), sizeof(address)) < 0) {
        cerr << "Binding error" << endl;
        return;
    }

    if (listen(server, 10) == -1) {
        cerr << "fail listening" << endl;
        return;
    }

    cout << "Server started" << endl;

    sockaddr_in clientAddress{};
    socklen_t clientAddrLen = sizeof(clientAddress);
    while (true){
        int clientSocket = accept(server, reinterpret_cast<struct sockaddr *>(&clientAddress), &clientAddrLen);
        if(clientSocket == -1){
            cout << "connection fail" << endl;
            continue;
        }
        cout << "Client " << clientAddress.sin_addr.s_addr << "  was connected" << endl;
        thread( requestProcess, clientSocket, clientAddress, structureJSON).detach();
    }
    close(server);
}


int main() {
    startServer();
    return 0;
}