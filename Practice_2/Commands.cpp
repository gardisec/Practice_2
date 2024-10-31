#include "Head.h"
#include <unistd.h>

void Insert(const string& table, arr<string>& data, const arr<string>& paths) {

	string correctPath = tablenameToPath(table, paths);// Считываем данные с файла json
	string nameSchema = readSchema("schema.json").pointer[0];
	string pathToLock = nameSchema + '/' + table + '/' + table + "_lock.txt";

	string tempString, pk;

	ifstream lockFile(pathToLock);//проверка на блокировку файла
	getline(lockFile, tempString);
	lockFile.close();

	if (tempString == "0") {// если разблокирован файл, то начинаем работу с ним
		
		lock(pathToLock);
		string line;
		ifstream file(correctPath);//читаем первую строчку
		if (file.is_open()) {
			getline(file, line);
			file.close();
		}
		arr<string> maxColums = splitString(",", line);//сплитим строчкуу по , и можем обратиться к её размеру

		if (maxColums.currSize - 1 == data.currSize) {// если кол-во данных в запросе равняется максимальному кол-ву данных в строке файла
			
			ifstream pkCount(nameSchema + '/' + table + '/' + table + "_pk_sequence.txt");//Находим номер строчки
			getline(pkCount, pk);
			pkCount.close();

			
			ofstream file(correctPath, ios::ate | ios::app);//записываем номер строчки и данные
			file << pk << ",";
			for (int j = 0; j < data.currSize; j++) {
				file << data.pointer[j];
				if (j != data.currSize - 1) {
					file << ",";
				}
			}
			file << "\n";
			file.close();
		}
		else { 
			cerr << "Incorrect numbers of values" << endl;
		}
		
		unlock(pathToLock);

		ofstream pkFileRecord(nameSchema + '/' + table + '/' + table + "_pk_sequence.txt");//инкрементируем pk и записываем в файл
		pkFileRecord << toInt(pk) + 1;
		pkFileRecord.close();

	}
	else if (tempString == "1") {//если над файлом уже работают, то выходим
		sleep(500);
		Insert(table, data, paths);
	}

}//INSERT INTO table2 VALUES ('zxczxb', 'asdasdg')

string Select(string& fromTable, string SelectData, const arr<string>& paths) {
	string nameSchema = readSchema("schema.json").pointer[0];
	string pathToLock, numLock;

	arr<string> allChose = splitString(",", SelectData);// заспличен запрос 
	arr<string> temp, empty, splitedChose;

	for (int i = 0; i < allChose.currSize; i++) {// на выходе получаем массив таблица , колонка ; след запрос тааблица , колонка 
		temp = splitString(".", allChose.pointer[i]);
		for (int j = 0; j < temp.currSize; j++) {
			splitedChose.push_back(temp.pointer[j]);
		}

	}
	temp = empty;//очистка массива
	int numColumn, count;
	string currentPath, column, stringFromFile;
	arr<string> tables = splitString(",", fromTable);// массив с названиями таблиц в которых работаем
	for (int i = 0; i < tables.currSize; i++) {

		currentPath = tablenameToPath(tables.pointer[i], paths);//определяем путь до .csv файла по азванию таблицы 
		pathToLock = nameSchema + '/' + tables.pointer[i] + '/' + tables.pointer[i] + "_lock.txt";

		for (int j = 0; j < splitedChose.currSize; j++) {
			count = 0;
			if (tables.pointer[i] == splitedChose.pointer[j]) {// если название таблицы совпадает, то следующий элемент в массиве это колонка к которой обращаемся
				column = splitedChose.pointer[j + 1];
				numColumn = column[column.size() - 1] - 48;// находим номер колонки к которой обращаемся

			}
			else {
				continue;
			}

			ifstream lockFile(pathToLock);//проверка на блокировку файла
			getline(lockFile, numLock);
			lockFile.close();

			if (numLock == "0") {
				lock(pathToLock);

				ifstream file(currentPath);

				if (file.is_open()) {
					while (getline(file, stringFromFile)) {// считываем строчку
						if (file.eof()) {// если конец файла то выходим
							file.close();
							break;
						}
						if (count == 0) {//если строчка первая в файле то пропускаем
							count++;
							continue;
						}
						else {//добавляем в массив номер строчки и элемент строчки находящийся в нужной колонке через запятую
							temp.push_back(splitString(",", stringFromFile).pointer[0] + "," + splitString(",", stringFromFile).pointer[numColumn]);
						}

					}
					temp.push_back(",");// ставим запятую, что ограничивает данные взятые по 1 запросу
				}

				unlock(pathToLock);
			}
			else {
				cerr << "File" << currentPath << "  already is open" << endl;
			}


		}
	}

	arr<string> indexTemp;
	for (int i = 0; i < temp.currSize; i++) {// заполняем массив с индексами окончания данных одного запроса
		if (temp.pointer[i] == ",") {
			indexTemp.push_back(toStr(i));
		}
	}
	arr<string> left, right;
	string result = "";
	for (int i = 0; i < toInt(indexTemp.pointer[0]); i++) {//делаем вывод для двух колонок
		for (int j = toInt(indexTemp.pointer[0]) + 1; j < toInt(indexTemp.pointer[1]); j++) {//проходит по первой колонке
			left = splitString(",", temp.pointer[i]);
			right = splitString(",", temp.pointer[j]);
			result += " \t" +left.pointer[0] + "   " + left.pointer[1] + " \t" + right.pointer[0] + "   " + right.pointer[1] + "\n";
		}
	}
	return result;
}
//SELECT table2.column1,table2.column2 FROM table2

void Delete(const string& table,const string& dataFrom,const string& filter,const arr<string>& path) {
	string currentPath = tablenameToPath(table, path);// определяем путь до файла над которым работаем

	string nameSchema = readSchema("schema.json").pointer[0];// определяем имя схемы
	string tempString;
	string pathToLock = nameSchema + '/' + table + '/' + table + "_lock.txt";
	ifstream lockFile(pathToLock);// получаем ключ блокировки
	getline(lockFile, tempString);
	lockFile.close();

	if (tempString == "0") {// если разблокировано, то работаем

		lock(pathToLock);

		arr<string> whereDel = splitString(".", dataFrom);// определяем колонку для проверки
		string column;
		int numColumn;
		for (int i = 0; i < whereDel.currSize; i++) {// цикл определяющий индекс элемента нужной колонки
			if (table == whereDel.pointer[i]) {
				column = whereDel.pointer[i + 1];
				numColumn = column[column.size() - 1] - 48;
				break;
			}
			else {
				continue;
			}
		}
		string stringFromFile, element;

		fstream file(currentPath, ios::in | ios::out | ios::binary);
		arr<string> buffer;//создаем буффер в который будем записывать все строчки которые не подходят под фильтр
		if (file.is_open()) {
			while (getline(file, stringFromFile)) {
				if (file.eof()) {
					break;
				}
				element = splitString(",", stringFromFile).pointer[numColumn];

				if (element == filter) {

					continue;
				}

				if (element != filter) {
					buffer.push_back(stringFromFile);
				}

			}


		}

		file.close();
		if (!file.is_open()) { // записываем всё из буфера в файл с самого начала
			ofstream file(currentPath);
			file.seekp(0, ios::beg);
			for (int i = 0; i < buffer.currSize; i++) {
				file << buffer.pointer[i];
			}

			file.close();
		}

		unlock(pathToLock);
	}
	else if (tempString == "1"){// если заблокирован то выходим
		
		cout << "File already is open. Try again letter." << endl;
	}
	//DELETE FROM table2 WHERE table2.column1 = '123'

}


string SelectWhere(string& fromTable, string SelectData, const arr<string>& paths, string whereRequest) {

	string nameSchema = readSchema("schema.json").pointer[0];
	string pathToLock;
	arr<string> empty;

	string currentPath, line, column, numLock;
	arr<string> splitedForOR = splitToArr(whereRequest, "OR");// сплитим фильтр по ИЛИ

	arr<string> splitedForAND, splitedForCompare, splitedForDot;

	bool isFirstStr, isHere;
	int numColumn;
	string strFilter;
	arr<string> tempColums, tempNumsString, splitedLine;

	arr<string> NumsStrAftCompFirst, NumsStrAftCompScnd, numsStringAfterAND, resultNums;

	for (int i = 0; i < splitedForOR.currSize; i++) {// каждую часть сплитим по И
		splitedForAND = splitToArr(splitedForOR.pointer[i], "AND");
		
		for (int j = 0; j < splitedForAND.currSize; j++) { //Каждую часть сплитим по = 

			if (NumsStrAftCompScnd.currSize > 0) { // если выражения слева и справа есть то сравниваем их по И
				for (int k = 0; k < NumsStrAftCompFirst.currSize; k++) {
					for (int z = 0; z < NumsStrAftCompScnd.currSize; z++) {
						if (NumsStrAftCompFirst.pointer[k] == NumsStrAftCompScnd.pointer[z]) {
							numsStringAfterAND.push_back(NumsStrAftCompFirst.pointer[k]);
						}
					}
				}
				NumsStrAftCompScnd = empty;// очищаем второй операнд
				NumsStrAftCompFirst = numsStringAfterAND;// первому присваиваем полученное значение
				numsStringAfterAND = empty; // очищаем полученное значение
			}
			splitedForCompare = splitString("=", splitedForAND.pointer[j]);

			if (splitedForCompare.pointer[0][0] == '\'') {// если левый элемент начинается с '

				strFilter = splitString("'", splitedForCompare.pointer[0]).pointer[1]; // то получаем фильтр для колонки

				splitedForDot = splitString(".", splitedForCompare.pointer[1]); // определяем колонку и таблицу справа от =

				currentPath = tablenameToPath(splitedForDot.pointer[0], paths);
				pathToLock = nameSchema + '/' + splitedForDot.pointer[0] + '/' + splitedForDot.pointer[0] + "_lock.txt";


				column = splitedForDot.pointer[1];
				numColumn = column[column.size() - 1] - 48;// определение индекса нужной колонки

				ifstream lockFile(pathToLock);//блокироваки файла
				getline(lockFile, numLock);
				lockFile.close();

				if (numLock == "0") {
					lock(pathToLock);

					ifstream file(currentPath);

					isFirstStr = true;
					while (getline(file, line)) {
						if (isFirstStr) { // если первая строчка файла то пропускаем
							isFirstStr = false;
							continue;
						}
						else {  
							splitedLine = splitString(",", line); // иначе сплитим строчку по ,
							if (strFilter == splitedLine.pointer[numColumn] && j == 0) { // если первая итерация и фильтр совпадает со значением в колонке, то добавляем номер колонки в первый массив
								NumsStrAftCompFirst.push_back(splitedLine.pointer[0]);
							}
							else if (strFilter == splitedLine.pointer[numColumn] && j != 0) { // если итерация не первая и фильтр совпадает со значением в строке, то добавляем номер колонки во второй массив
								NumsStrAftCompScnd.push_back(splitedLine.pointer[0]);
							}
						}
					}

					file.close();

					unlock(pathToLock);
				}
				else {
					cerr << "File" << currentPath << "  already is open" << endl;
				}
				

			}
			else if (splitedForCompare.pointer[1][0] == '\'') { // если правый элемент начинается с ' 
				strFilter = splitString("'", splitedForCompare.pointer[1]).pointer[1];

				splitedForDot = splitString(".", splitedForCompare.pointer[0]); // то определяем таблицу и колонку левого элемента

				currentPath = tablenameToPath(splitedForDot.pointer[0], paths);
				pathToLock = nameSchema + '/' + splitedForDot.pointer[0] + '/' + splitedForDot.pointer[0] + "_lock.txt";

				column = splitedForDot.pointer[1];
				numColumn = column[column.size() - 1] - 48;// определяем индекс нужной колонки

				ifstream lockFile(pathToLock);//блокировка файла
				getline(lockFile, numLock);
				lockFile.close();
				if (numLock == "0") {
					lock(pathToLock);
					ifstream file(currentPath);

					isFirstStr = true;
					while (getline(file, line)) {// если первая строка то пропускаем её
						if (isFirstStr) {
							isFirstStr = false;
							continue;
						}
						else { // иначе читаем строку
							splitedLine = splitString(",", line);
							if (strFilter == splitedLine.pointer[numColumn] && j == 0) {// если первая итерация и фильтр совпадает со значением в колонке, то добавляем номер колонки в первый массив
								NumsStrAftCompFirst.push_back(splitedLine.pointer[0]);
							}
							else if (strFilter == splitedLine.pointer[numColumn] && j != 0) {// если итерация не первая и фильтр совпадает со значением в строке, то добавляем номер колонки во второй массив
								NumsStrAftCompScnd.push_back(splitedLine.pointer[0]);
							}
						}
					}

					file.close();
					unlock(pathToLock);
				}
				else {
					cerr << "File" << currentPath << "  already is open" << endl;
				}
				
			}
			else { // если оба элемента сравнения содержат названия таблиц и колонок

				for (int k = 0; k < splitedForCompare.currSize; k++) { // сплитим каждую часть по . ( получаем название таблицы и колонки)


					splitedForDot = splitString(".", splitedForCompare.pointer[k]);

					currentPath = tablenameToPath(splitedForDot.pointer[0], paths);
					pathToLock = nameSchema + '/' + splitedForDot.pointer[0] + '/' + splitedForDot.pointer[0] + "_lock.txt";

					column = splitedForDot.pointer[1];
					numColumn = column[column.size() - 1] - 48; // получаем индекс нужной колонки

					ifstream lockFile(pathToLock);//блокировка файла
					getline(lockFile, numLock);
					lockFile.close();
					if (numLock == "0") {
						lock(pathToLock);

						ifstream file(currentPath);

						isFirstStr = true;
						while (getline(file, line)) {// если первая строка то пропускаем
							if (isFirstStr) {
								isFirstStr = false;
								continue;
							}
							else { // иначе читаем строку
								splitedLine = splitString(",", line); // сплитим строчку по , 
								tempColums.push_back(splitedLine.pointer[numColumn]); //добавляем значение необходимой колонки
								tempNumsString.push_back(splitedLine.pointer[0]); // добавляем соответственно номер строки в другой массив
							}
						}

						file.close();
						unlock(pathToLock);
					}
					else {
						cerr << "File" << currentPath << "  already is open" << endl;
					}
					
				}

				for (int k = 0; k < tempColums.currSize / 2; k++) { // цикл для проверки соответствия условия равенства
					if (tempColums.pointer[k] == tempColums.pointer[tempColums.currSize / 2 + k] && j == 0) { // если итерация первая то в первый массив номер строки
						NumsStrAftCompFirst.push_back(tempNumsString.pointer[k]);
					}
					else if (tempColums.pointer[k] == tempColums.pointer[tempColums.currSize / 2 + k] && j != 0) { // если итерация не первая то во второй массив номер строки
						NumsStrAftCompScnd.push_back(tempNumsString.pointer[k]);
					}
				}
				tempColums = empty; // очищаем массивы
				tempNumsString = empty;
				
			}
		
		}
		
		for (int j = 0; j < NumsStrAftCompFirst.currSize; j++) { // цикл для проверки условия ИЛИ
			isHere = true;
			for (int k = 0; k < resultNums.currSize; k++) { // если находится такой же элемент из второго массива в первом массиве
				if (NumsStrAftCompFirst.pointer[j] == resultNums.pointer[k])
					isHere = false; 
			}
			if (isHere) { // то он не попадет в новый массив
				resultNums.push_back(NumsStrAftCompFirst.pointer[j]);
			}
			
		}
	}



	arr<string> selectArr = splitString(",", SelectData);
	arr<string> result, indexResult;
	
	for (int i = 0; i < selectArr.currSize; i++) { // цикл для определения элементов для вывода
		
		splitedForDot = splitString(".", selectArr.pointer[i]);

		currentPath = tablenameToPath(splitedForDot.pointer[0], paths);
		pathToLock = nameSchema + '/' + splitedForDot.pointer[0] + '/' + splitedForDot.pointer[0] + "_lock.txt";

		column = splitedForDot.pointer[1];
		numColumn = column[column.size() - 1] - 48; // определяем индекс колонки

		
		for (int j = 0; j < resultNums.currSize; j++) {

			ifstream lockFile(pathToLock);//блокировка файла
			getline(lockFile, numLock);
			lockFile.close();
			if (numLock == "0") {
				lock(pathToLock);

				ifstream file(currentPath);

				isFirstStr = true;
				while (getline(file, line)) {// если первая строка то пропускаем
					if (isFirstStr) {
						isFirstStr = false;
						continue;
					}
					else { // иначе сплитим по , 
						splitedLine = splitString(",", line);
						if (splitedLine.pointer[0] == resultNums.pointer[j]) { // если номер из условия совпадает с номером текущей строки то добавляем 
							indexResult.push_back(splitedLine.pointer[0]); // номер строки
							result.push_back(splitedLine.pointer[numColumn]); // содержимое колонки

							break;// выходим так как строки не повторяются
						}

					}
				}
				file.close();

				unlock(pathToLock);
			}
			else {
				cerr << "File" << currentPath << "  already is open" << endl;
			}
			
		}
		
	}

	string toReturn = "";
	for (int i = 0; i < indexResult.currSize / 2; i++) {//вывод декартового произведения данных нужных колонок
		for (int j = indexResult.currSize / 2; j < indexResult.currSize; j++) {
			toReturn += indexResult.pointer[i] + "\t" + result.pointer[i] + "\t\t" + indexResult.pointer[j] + "\t" + result.pointer[j] + "\n";
		}
	}

	return toReturn;
	//SELECT table2.column1,table1.column2 FROM table1,table2 WHERE table1.column1 = table1.column2 AND table2.column1 = table2.column2 AND table1.column1 = '123' OR table2.column2 = '123'
}