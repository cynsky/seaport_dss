# seaport_dss

Скрипт берет открытые данные AIS с сайта marinecadastre.gov. С помощью которых строятся статистические распределения по типу судна, размеру и токсичности груза.

Для работы скрипта требуются библиотеки: pandas, requests, matplotlib, gdal.

##Аргументы:
* zone (обязательный) - зона UTC, целое число от 1 до 20
* year (обязательный) - год, целое число от 2010 до 2014
* --month - месяц или список месяцев, если не указан, берутся данные за все 12 месяцев
* --folder - папка для сохранения данных, если не указана используется папке data в текущей директории