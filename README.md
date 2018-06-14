# Huizen Holland

## What is Huizen Holland?
Huizen Holland is a project used to split data which contains information 
    about how many houses there are/or were in an area in a specific year.
The new data can then be used for research on the multiple villages and the cities.

## Usage
In order to use the code to process the data there are two things that can be done. These are:

- Downloading the code directly from github to a local folder.
    - Have an IDE to open the code and run it from within the IDE.
- Call the java application by using the command line.
    - Have the java application downloaded and ready for use.

Remember to make sure the files needed exist in the same folder as the application/code and are in the correct format for the program to handle them.

#### Raw data
The format for the first file, e.g. the database with the raw information about the number of houses, 
    to pass as an argument is as follows:

| YEAR   | LINK   | HOUSES   | KM2   |
|:------:|:------:|:--------:|:-----:|
| 1840   | HO1351 | 1353     |12.4549|
| 1840   | HO1351 | 1353     |12.4549|
| 1840   | HO1351 | 1353     |12.4549|

The YEAR column contains the year of the record, e.g. the year when the initial measurement was done.
The LINK column contains the Link(s) that make up the location(s) used for the initial measurement.
The HOUSES column contains the number of houses that were present in that area in that year.
The KM2 column contains the amount of square kilometres that spans the location.

#### Square Kilometres 
The format for the second file, e.g. the database with the square kilometres per split location (LINK)
    to pass as an argument is as follows:

| SHORT ID |  1477   |  1632   |  1840   |
|:--------:|:-------:|:-------:|:-------:|
| HO1351   | 12.4549 | 12.4549 | 12.4549 |
| HO1351A  | 7.8463  | 7.8463  | 7.8463  |
| HO1351B  | 4.6351  | 4.6351  | 4.6351  |    

The SHORT ID column contains the Link that makes up the location for which the amount of square kilometres is for.
The 1477 column contains the amount of square kilometres for the SHORT ID.
The 1632 column contains the amount of square kilometres for the SHORT ID.
The 1840 column contains the amount of square kilometres for the SHORT ID.

Note. there can be more year columns than represented in the example above.

#### Export file
The export file does not have to have a specific format, or even to exist before usage of the code.
This is all handled by the program itself. Therefore only a pathname needs to be given for the export file to be called.

#### Notes file
The notes file does not have to have a specific format, or even to exist before usage of the code.
This is all handled by the program itself. Therefore only a pathname needs to be given for the notes file to be called.

## Export File
The export file is the file that contains the processed data which can then be used for research.
The format given to the export file by the program will be about the same as the square kilometres data file, namely:

| CODE     |  1477   |  1632   |  1840   |
|:--------:|:-------:|:-------:|:-------:|
| HO1351   | 13543   | N/A     | N/A     |
| HO1351A  | N/A     | 9546    | 12896   |
| HO1351B  | N/A     | 6543    | 8213    |

The CODE column contains the Link code of the location for which the data is collected.
The 1477 column contains the amount of houses for that location in that year.
The 1632 column contains the amount of houses for that location in that year.
The 1840 column contains the amount of houses for that location in that year.

Note. there can be more year columns than represented in the example above.

After the export, this file can be used by researchers to collect information about the number
of homes for the given Link code. This Link code can then be combined with other information to get
statistics about a certain location.

## Notes File
The notes file is the file that contains the notes for the way the raw data for a specific location
is processed during the run of the program. The format given to the export by the program will be same
as the export file, namely:
 
| CODE     |  1477   |  1632                  |  1840           |
|:--------:|:-------:|:----------------------:|:---------------:|
| HO1351   | Bron    | N/A                    | N/A             |
| HO1351A  | N/A     | Jaar 1632: Oppervlakte | Jaar 1632: Bron |
| HO1351B  | N/A     | Jaar 1632: Oppervlakte | Jaar 1632: Bron |

Note. there can be more year columns than represented in the example above.

After the export, this file can be used by researcher to determine how the calculation was done
for splitting the number of houses per location. In this way it is possible to determine how large
the deviation could be for the specific location and year.


##Calling the script via command line
In order to call the script via the command line, one needs to either open the command prompt or powershell.

Command Prompt: Click the Windows logo → Windows System → Command Prompt or press the Windows logo on the keyboard and start typing "command prompt" and then pressing "Enter".
Powershell: Click the Windows logo → Windows Powershell → Windows Powershell or press the Windows logo on the keyboard and start typing "(Windows) Powershell" and the pressing "Enter".

When one of these is opened it will show the current directory it is in like: "C:\Users\[your username]>". From here you will need to navigate to the directory that contains the application. This can be done by using the following command:
- "cd .\<Directory to go to>\<potential subdirectory to go to>\" and then press "Enter"

After using the command the directory displayed should be something like "C:\Users\[username]\Desktop\Huizen Holland>".
From here it is possible to check the files present in that directory by using the following command:
- For Command Prompt: "dir"
- For Powershell: "ls"
These commands will display all the files present.

If checked if the java application is present in the current directory, it can be called by using the following command:
- "java -jar .\EMHCD.jar '.\Huizen Holland Database (2018-02-28)_compact.csv' '.\km2 - Huizen Holland Database (2018-05-30).csv' .\output\ .\output\"

This command calls the java installed on the computer and gives the argument a 'JAR' file will be run and the name of the JAR file being ".\EMHCD.jar". Along with this call a couple of parameters are given, namely:
- '.\Huizen Holland Database (2018-02-28)_compact.csv' → the data set to be converted. The format for this can be found above under the heading "Raw data".
- '.\km2 - Huizen Holland Database (2018-05-30).csv' → the data of the km2 to be used for converting the raw data set. The format for this can be found above under the heading "Square Kilometres".
- .\output\ → The output directory for the processed data. The format used by the application can be found under "Export File".
- .\output\ → The output directory for the notes data. The format used by the application can be found under "Notes File".

When running, the application will display information on the screen to show what is happening during the process.

As soon as the application is done, the output files can be found under the directory given when calling the application. The naming convention of the output files will be something like this:
- Export File: "Early Modern House Count Disaggregation Export 20180611T165137.csv"
- Notes File: "Early Modern House Count Disaggregation Export Notes 20180611T165137.csv"
