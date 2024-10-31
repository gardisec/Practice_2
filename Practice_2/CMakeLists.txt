cmake_minimum_required(VERSION 3.25)
project(Practice_2)
project(Connections)

set(CMAKE_CXX_STANDARD 17)

add_executable(Practice_2
        arr.cpp
        Commands.cpp
        helpFunc.cpp
        parsers.cpp
        serverFuncs.cpp

)
add_executable(Connections
        Connections.cpp
)
