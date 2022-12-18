import os
import json
import pickle
from pathlib import Path
from numpy import genfromtxt
from geopy.geocoders import Nominatim

folder_name = "data_files/normals-annualseasonal"
folder = Path(folder_name)

geolocator = Nominatim(user_agent="Tester")
data_arr = []
broken_data = []

def refillData():
    print("Start refill data")
    tempData_arr = []
    temp_brokendata = []
    #Заполнение массива .cvs файлами
    for root, dirs, files in os.walk(folder_name):  
        for i in files:
            temp_arr = []
            temp_dict = {}

            f = open(folder_name+'/'+i, "r")

            #добавление в массив 2х строчек в виде массива строк
            temp_arr.append(f.readline().rstrip().split(','))
            temp_arr.append(f.readline().rstrip().split(',"'))

            #зачистка ненужных кавычек в строках
            for j in temp_arr:
                for st in range(len(j)):
                    j[st] = j[st].replace('"', '')

            #в некоторых файлах, во втоой строке, дублируются запятые и получаем пустые строки в данных
            #проверка битых файлов
            if (len(temp_arr[0])!=len(temp_arr[1])):
                temp_brokendata.append(i)
                #print(temp_arr[1])
                continue

            #перевод temp_arr в словарь key:value для более удобного поиска по названию поля + подрезание второй строки (как дополнение)
            for k in range(len(temp_arr[0])):               
                temp_dict[temp_arr[0][k]] = temp_arr[1][k]

            tempData_arr.append(temp_dict)

    with open('data_files/broken_data.txt', 'w') as fw:
        json.dump(temp_brokendata, fw)
    print("Refill done")
    return tempData_arr

with open('data_files/data.bin', 'rb') as fr:
    # читаем из файла
    try:
        data_arr = pickle.load(fr)
    except:
        print ('data file is empty')
        #перезапись
        with open('data_files/data.bin', 'wb') as fw:
            data_arr = refillData()
            pickle.dump(data_arr, fw)
        
with open('data_files/broken_data.txt', 'r') as br:
    try:
        broken_data = json.load(br)
        for s in broken_data:
            print("File "+s+" contains broken data") 
    except:
        print ("There isn't broken data")


if(len(data_arr)+len(broken_data) != len(list(folder.iterdir()))):
    print("----------------------------------------------\nThe number of source files does not match the number of saved files")
    with open('data_files/data.bin', 'wb') as fw:
        data_arr = refillData()
        pickle.dump(data_arr, fw)

    with open('data_files/broken_data.txt', 'r') as br:
        broken_data = json.load(br)
    
        
print("----------------------------------------------")

#Функция для получения значения по ключу, в определенном массиве  
def findValue (key, temp_arr):
    if key in temp_arr:
        return(temp_arr[key])
    else:
        return(False)

#поиск min и max по key = 'prop'
prop = 'ANN-CLDD-NORMAL'

min = max = int(findValue(prop,data_arr[0]))
station_location_min = station_location_max = findValue("LATITUDE",data_arr[0])+', '+findValue("LONGITUDE",data_arr[0]) #широта и долгота
temp_station_min = temp_station_max = ''
fileName_min = fileName_max = ''
for i in data_arr:

    temp_value = int(findValue(prop,i))
    temp_location = findValue("LATITUDE",i)+', '+findValue("LONGITUDE",i)
    temp_name = findValue('STATION',i)
    #temp_station = i[station_key]

    #min
    if min>temp_value:
        min = temp_value
        station_location_min = temp_location
        fileName_min = temp_name

    #max
    if max<temp_value:
        max = temp_value
        station_location_max = temp_location
        fileName_max = temp_name

location_min = geolocator.reverse(station_location_min)
location_max = geolocator.reverse(station_location_max)

print("Property: " + prop)
#print (station_location_min, station_location_max)
print("\nFile (min): " + fileName_min+".csv\nMin value: " + str(min) + "\nLocation with min value: "+location_min.address)
print("\nFile (max): " + fileName_max+".csv\nMax value: " + str(max) + "\nLocation with max value: "+location_max.address)
#print (len(data_arr))



