#include "Head.h"

arr<string> readSchema(const string& filePath) {// функция возвращающая массив с данными из json файла

    json schema;

    ifstream file(filePath);
    if (file.is_open()) {// читаем json файл
        file >> schema;
        file.close();
    }
    else {
        cerr << "Не удалось открыть файл: " << filePath << endl;
    }
    arr<string> jsonSettings;// заполняем массив прочитанными данными

    jsonSettings.push_back(schema["name"]);
    jsonSettings.push_back(",");

    jsonSettings.push_back(toStr(schema["tuples_limit"]));
    jsonSettings.push_back(",");
    
    for (json::iterator it = schema["structure"].begin(); it != schema["structure"].end(); ++it) {// цикл заполняющий название таблиц и колонок в массив 
        jsonSettings.push_back(it.key());

        for (auto temp : schema["structure"][it.key()]) {
            jsonSettings.push_back(temp);
        }

        jsonSettings.push_back(",");
    }
    return jsonSettings;
}

string request(const string& strToRequest) {// функция которая определяет запрос и далее вызывает нужную функцию

    arr<string> path = createCSV();// создаются все файлы и возвращаются все пути до нужных csv файлов(подходящих под условие)

    arr<string> commandWords = splitString(" ", strToRequest);// сплитим запрос по пробелам, для дальнейшего определения команды

    string result;
    if (commandWords.pointer[0] == "SELECT" && commandWords.pointer[2] == "FROM" && commandWords.pointer[4] == "WHERE") {// если селект с фильтром
        try {

            string whereRequest = "";
            for (int i = 5; i < commandWords.currSize; i++) {// записываем фильтр в строку
                whereRequest += commandWords.pointer[i];
            }

            result = SelectWhere(commandWords.pointer[3], commandWords.pointer[1], path, whereRequest);
        }
        catch (const exception& error) {
            cerr << error.what() << endl;
        }
    }
    else if (commandWords.pointer[0] == "SELECT" && commandWords.pointer[2] == "FROM"){// если обычный селект
        try {

            result = Select(commandWords.pointer[3], commandWords.pointer[1], path);
        }
        catch (const exception& error) {
            cerr << error.what() << endl;
        }
    }
    else if (commandWords.pointer[0] == "INSERT" && commandWords.pointer[1] == "INTO" && commandWords.pointer[3] == "VALUES" ){ // если инсерт
        try {
            arr<string> data;
            arr<string> temp = splitString("'", strToRequest);// сплитим запрос по ' , чтобы определить данные для инсерта

            for (int i = 0; i < temp.currSize; i++) {// данные для инсерта находятся по индексам с нечетным значением
                if (i % 2 == 1) {
                    data.push_back(temp.pointer[i]);
                }
            }
            Insert(commandWords.pointer[2], data, path);
            result = "Insert complete";
        }
        catch (const exception& error) {
            cerr << error.what() << endl;
        }
    }
    else if (commandWords.pointer[0] == "DELETE" && commandWords.pointer[1] == "FROM" && commandWords.pointer[3] == "WHERE" && commandWords.pointer[5] == "=") {// если делит
        try {
            string table = commandWords.pointer[2]; // определяем таблицу для работы
            string dataFrom = commandWords.pointer[4]; // колонку для фильтра
            
            string filter = splitString("'", strToRequest).pointer[1];// определяем фильтр для удаления
            Delete(table, dataFrom, filter, path);
            result = "Delete complete";
        }
        catch (const exception& error) {
            cerr << error.what() << endl;
        }
    }
    else if (commandWords.pointer[0] == "EXIT") {// если написать EXIT, то выход из программы
        exit(1);
    }
    else {
        result = "Undefined command\n";
    }

    return result;
    
}