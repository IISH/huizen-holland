# Huizen Holland

## What is Huizen Holland?
Huizen Holland is a project used to split data which contains information 
    about how many houses there are/or were in an area in a specific year.
The new data can then be used for research on the multiple villages and the cities.

## Usage
In order to use the code to process the data there are a couple of things that need to be done.
These are:

- Have an IDE to open the code and run it from the IDE or use the command line to execute the Java file
- Download the project from github to a local folder
- Have the data to process available for usage, which needs to be referred to by
  * editing the configuration to comply the format used in the code
  * referring to the files needed via the command line as parameters (still work in progress)
  
Remember to make sure the files are in the correct format for the program to handle them.

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

