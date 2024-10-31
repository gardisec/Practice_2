#include "Connections.h"

int main() {

    char buff[1024];//задаем буфер
    const int sock = socket(AF_INET, SOCK_STREAM, 0); //определяем сокет
    if (sock == -1) {
        cerr << "Socket creation fail" << endl;
        return -1;
    }
    struct sockaddr_in server = {};
    server.sin_family = AF_INET; //определяем ip
    server.sin_port = htons(7432);// открываем порт
    if (inet_pton(AF_INET, "127.0.0.1", &server.sin_addr) <= 0) { //проверка на корректность адреса
        cerr << "Invalid address" << endl;
        return -1;
    }
    if (connect(sock, reinterpret_cast<sockaddr *>(&server), sizeof(struct sockaddr_in)) < 0) {//пробуем подключить
        cerr << "Connection failed" << endl;
        return -1;
    }
    cout << "Connected" << endl;
    string buffer;

    bool isExit = false;
    while (!isExit) {
        cout << "Enter your request" << endl;
        getline(cin, buffer);
        if (buffer == "disconnect") {
            isExit = true;
            send(sock, buffer.c_str(), buffer.size() - 1, 0);
            continue;
        }
        send(sock, buffer.c_str(), buffer.size(), 0);
        auto recvLen = recv(sock, &buff, sizeof buff, 0);
        if (recvLen == -1) {
            cerr << "read failed" << endl;
            continue;
        }
        if (recvLen == 0) {
            cerr << "EOF occured" << endl;
            continue;
        }
        buff[recvLen] = '\0';
        cout << buff << endl;
    }
    return 0;
}
