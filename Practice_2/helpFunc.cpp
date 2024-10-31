#include "Head.h"

void lock(const string& path) {
	ofstream lockFile(path);
	lockFile << "1";
	lockFile.close();
}

void unlock(const string& path) {
	ofstream lockFile(path);
	lockFile << "0";
	lockFile.close();
}


string toStr(const int& value) { // функция переводящая число в строку
	string result;
	int digit = value;
	while (digit > 0) {
		int temp = digit % 10;
		result = static_cast<char>(temp + '0') + result;
		digit /= 10;
	}
	return result;
}

int toInt(const string& str) { // функция переводящая строку в число
	int result = 0;
	for (int i = 0; i < str.size(); i++) {
		result = result * 10 + static_cast<int>(str[i] - '0');
	}
	return result;
}

arr<string> splitString(const string value, const string& str) {// функция разбивающая строку по заданному символу(строке)

	arr<string> result;
	size_t begin = 0;
	size_t end = str.find(value);

	while (end != -1) {
		result.push_back(str.substr(begin, end - begin));
		begin = end + 1;
		end = str.find(value, begin);
	}
	result.push_back(str.substr(begin, end));
	return result;
}

arr<string> splitToArr(const string& input, const string& delimiter) {
	string word;
	arr<string> output;
	bool isDelim;
	int j;
	for (int i = 0; i < input.size(); ++i) {
		if (input[i] == delimiter[0]) {
			isDelim = true;
			for (j = 0; j < delimiter.size(); ++j) {
				if (delimiter[j] != input[i + j]) isDelim = false;
			}
			if (isDelim) {
				output.push_back(word);
				word = "";
				i += j - 1;
			}
			else {
				word += input[i];
			}
		}
		else {
			word += input[i];
		}
	}
	if (word != "") {
		output.push_back(word);
	}
	return output;
}

string tablenameToPath(const string& table, const arr<string>& paths) {// функция которая определяет путь до нужного csv файла по названию таблицы

	string correctPath;
	arr<string> tempArray;
	for (int i = 0; i < paths.currSize; i++) {// проходим по всем путям

		tempArray = splitString("/", paths.pointer[i]); // сплитим по "/"

		for (int j = 0; j < tempArray.currSize; j++) {//если название таблицы совпадает то выводим путь
			if (table == tempArray.pointer[j]) {
				correctPath = paths.pointer[i];
				return correctPath;
			}
		}
	}
}

void createDirectory() {// функция создающая директории по json файлу

	arr<string> settings = readSchema("schema.json");// читаем json
	
	string name = settings.pointer[0];

	if (!fs::exists(name)) { // если нет папки с названием схемы, то создаём
		fs::create_directory(name);
	}

	bool isTableName = false;// пеоеменная показывающая является данная строка названием таблицы или нет
	for (int i = 3; i < settings.currSize; i++) {
		if (isTableName) { // если является таблицей то создаем директорию с названием данной таблицы (если не существует)
			if (!fs :: exists(name +  '/'  + settings.pointer[i])) {
				fs::create_directory(name + '/' + settings.pointer[i]);
			}

			isTableName = false;
		}
		if (settings.pointer[i] == ",") { // после , идет название таблицы следовательно возвращаем true
			isTableName = true;
			continue;
		}
			
	}
	
}

arr<string> createCSV() {// создание файлов и возвращение путей до незаполненных csv файлов

	createDirectory();// создаем все директории
	arr<string> settings = readSchema("schema.json");// определяем имя схемы, лимит строк, название колонок и таблиц
	string name = settings.pointer[0];
	const int tuple_limit = toInt(settings.pointer[2]);

	arr<string> pathToFiles; 
	string currPath, pathPKFile, pathLockFile, tempLine;
	int counter, j;
	int fileIndex = 1;
	bool isTableName = false;

	for (int i = 3; i < settings.currSize; i++) {
		if (settings.pointer[i] == ",") {// если это , то идем дальше 
			isTableName = true;
			continue;
		}
		else if (isTableName) {
			while (1) {// цикл определяющий незаполненный csv файл

				currPath = name + '/' + settings.pointer[i] + '/' + toStr(fileIndex) + ".csv"; //определяем путь до csv файла
				counter = 0;
				ifstream tuplLimit(currPath);

				while (getline(tuplLimit, tempLine)) {// считаем кол-во строк в нем
					
					counter++;
					
					
				}
				if (counter < tuple_limit + 1) { // если строк в файле больше чем лимит не считай заголовочную строку то проверяем следующий файл(если нет то создаем его)
					pathToFiles.push_back(currPath);
					break;
				}
				else {
					fileIndex++;
					continue;
				}
				
			}
			

			pathPKFile = name + '/' + settings.pointer[i] + '/' + settings.pointer[i] + "_pk_sequence.txt";// создание pk_sequence.txt файла и заполнение его начальными данными
			if (!fs::exists(pathPKFile)) {// создаем pk - файлы
				ofstream pkFile(pathPKFile);
				pkFile << "0";
				pkFile.close();
			}

			pathLockFile = name + '/' + settings.pointer[i] + '/' + settings.pointer[i] + "_lock.txt";// создание lock.txt файла и заполнение его начальными данными
			if (!fs::exists(pathLockFile)) {// создаем pk - файлы
				ofstream lockFile(pathLockFile);
				lockFile << "0";
				lockFile.close();
			}
			j = i + 1;

			if (!fs::exists(currPath)) {// заполнение заголовочной строки в csv файлах
				ofstream file(currPath);
				file << settings.pointer[i] << "_pk,";
				while (settings.pointer[j] != ",") {
					file << settings.pointer[j];
					if (settings.pointer[j + 1] != ",") {
						file << ",";
					}

					j++;
				}
				file << "\n";
				file.close();
			}
			isTableName = false;
		}

	}

	return pathToFiles; 
}

