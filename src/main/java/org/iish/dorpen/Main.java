package org.iish.dorpen;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.text.*;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    private static final TreeMap<String, Record> records = new TreeMap<>(); // Contains the records from the csv, String is the id of the record.
    private static Set<SquareKilometreRecord> squareKilometreRecords = new TreeSet<>();
    private static final Map<String, Set<String>> codesToIds = new HashMap<>(); // Contains information about link codes belonging to the ids
    private static final Map<String, Set<String>> codeHierarchy = new HashMap<>(); // Contains information about possible children of parents
    private static final List<String> codes = new ArrayList<>(); // Contains all the codes from the csv file
    private static BigDecimal numberOfHouses = new BigDecimal(0);
    private static int record_id_counter = 2;
    private static Set<String> recordsToRemove = new TreeSet<>();
    private static final Map<String, Record> recordsToAdd = new HashMap<>();
    private static final Set<Integer> years_from_data = new TreeSet<>();
    private static final SortedMap<String, VillageComplex> dorpenCollected = new TreeMap<>();

    private static final CSVFormat csvFormat = CSVFormat.EXCEL
            .withFirstRecordAsHeader()
            .withDelimiter(';')
            .withIgnoreEmptyLines()
            .withNullString("");

    public enum NoteState{
        SOURCE,
        YEAR,
        YEAR_SOURCE,
        YEAR_SURFACE,
        SURFACE,
        COMBINATION;

        public String getState(int year){
            switch (this){
                case SOURCE:
                    return "Bron";
                case YEAR:
                    return "Jaar " + Integer.toString(year);
                case YEAR_SOURCE:
                    return "Jaar " + Integer.toString(year) + ": Bron";
                case YEAR_SURFACE:
                    return "Jaar " + Integer.toString(year) + ": Oppervlakte";
                case SURFACE:
                    return "Oppervlakte";
                case COMBINATION:
                    return Integer.toString(year) + " + Oppervlakte";
                default:
                    return "Onbekend";
            }
        }
    }

    /**
     * The main method to start it all
     *
     * @param args String[] containing the file paths to load and the file path to write to
     * @throws Exception Exception for when the data is not valid to run the code
     */
    public static void main(String[] args) throws Exception {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        System.out.println(dateFormat.format(date)); //2016/11/16 12:08:43

        String importCsv = args[0];
        String importSquareKilometres = args[1];
        String exportCsv = args[2];
        String notesCsv = args[3];

        loadData(importCsv);
        loadSquareKilometers(importSquareKilometres);

        updateLinks();

        export(exportCsv, notesCsv);

        date = new Date();
        System.out.println(dateFormat.format(date)); //2016/11/16 12:08:43
    }

    /**
     * Converts the number given to a local BigDecimal
     *
     * @param number String The number to convert to the local format
     * @return BigDecimal The BigDecimal converted to the local format
     */
    private static BigDecimal convertToLocale(String number) {
        DecimalFormatSymbols symbols = new DecimalFormatSymbols();
        symbols.setDecimalSeparator(',');

        DecimalFormat df = new DecimalFormat("########,###", symbols);
        df.setGroupingUsed(false);
        return new BigDecimal(df.format(new BigDecimal(number).setScale(3, BigDecimal.ROUND_HALF_EVEN)));
    }

    /**
     * Loads the data from the csv file
     *
     * @param csvPath String The path to the CSV file which contains the data to be processed
     * @throws Exception Exception thrown when the data is incorrect
     */
    private static void loadData(String csvPath) throws Exception {
        CSVParser parser = CSVParser.parse(new File(csvPath), Charset.forName("UTF-8"), csvFormat);
        parser.forEach(record -> {
            Record r = new Record();
            r.id = Integer.toString(record_id_counter);
            r.year = new Integer(record.get("YEAR"));
            r.houses = record.get("HOUSES") != null ? new BigDecimal(record.get("HOUSES")) : null;
            numberOfHouses = r.houses != null ? numberOfHouses.add(r.houses) : numberOfHouses.add(new BigDecimal(0));
            r.km2 = record.get("KM2") != null ? convertToLocale(record.get("KM2")).setScale(3, BigDecimal.ROUND_HALF_EVEN) : null;
            r.note = NoteState.SOURCE;
            if (record.get("LINK") != null) {
                String[] links = record.get("LINK").split("-");
                for (String code : links) {
                    if (code.substring(0, 2).contains("HO")) {
                        r.links.add(code);
                        Set<String> ids = codesToIds.getOrDefault(code, new HashSet<>());
                        ids.add(r.id);
                        codesToIds.put(code, ids);

                        setParentRelation(code);
                    } else {
                        r.links.add(code);
                    }
                }
            } else {
                r.links.add("");
            }
            records.put(r.id, r);
            years_from_data.add(r.year);
            record_id_counter++;
        });
        System.out.println("Number of houses in total is: " + numberOfHouses);
    }

    private static void loadSquareKilometers(String csvPath) throws Exception {
        CSVParser parser = CSVParser.parse(new File(csvPath), Charset.forName("UTF-8"), csvFormat);
        Set<Integer> years = new TreeSet<>();
        for (String year : parser.getHeaderMap().keySet()) {
            if (!year.equals("SHORT-ID")) {
                years.add(Integer.parseInt(year));
            }
        }
        parser.forEach(record -> {
            SquareKilometreRecord s = new SquareKilometreRecord();
            s.linkCode = record.get("SHORT-ID");
            for (int year : years) {
                BigDecimal squareKm = record.get(Integer.toString(year)) != null ? new BigDecimal(record.get(Integer.toString(year))) : new BigDecimal(0);
                s.km2.put(year, squareKm);
            }
            squareKilometreRecords.add(s);
        });
    }

    /**
     * Sets the parent relation for each of the link codes
     *
     * @param code String The Link code to update
     */
    private static void setParentRelation(String code) {
        if (code.length() == 6)
            return;

        String parent = code.substring(0, code.length() - 1);
        Set<String> codes = codeHierarchy.getOrDefault(parent, new HashSet<>());
        codes.add(code);
        codeHierarchy.put(parent, codes);

        setParentRelation(parent);
    }

    /**
     * Updates the links for the records
     */
    private static void updateLinks() {
        new HashSet<>(codesToIds.keySet()).forEach(Main::updateLinksForCode);
    }

    /**
     * Updates the links so the lowest possible is given, and it is possible to work from down up
     *
     * @param code String The Link code to update
     */
    private static void updateLinksForCode(String code) {
        if (!codes.contains(code)) {
            codes.add(code);
        }
        if (codeHierarchy.containsKey(code)) {
            for (String parentCode : codeHierarchy.get(code)) {
                Set<String> ids = codesToIds.getOrDefault(parentCode, new HashSet<>());
                ids.addAll(codesToIds.get(code));
                codesToIds.put(parentCode, ids);

                updateLinksForCode(parentCode);
            }
            codesToIds.remove(code);
        }
    }

    /**
     * Exports the information to a csv file.
     *
     * @param exportPath String the path to which the export is exported
     * @throws Exception NullPointerException is caught when an exception is thrown.
     */
    private static void export(String exportPath, String notesPath) throws Exception {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(exportPath))) {
            CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat);

            List<String> headerRow = new ArrayList<>();
//            headerRow.add("YEAR");
//            headerRow.add("LINK");
//            headerRow.add("HOUSES");
//            headerRow.add("KM2");

            headerRow.add("Code");
            @SuppressWarnings("ComparatorMethodParameterNotUsed")
            List<String> temp_list = years_from_data.stream().sorted((t1, t2) -> (t1 <= t2) ? -1 : 1).map(map -> Integer.toString(map)).collect(Collectors.toList());
            headerRow.addAll(temp_list);

            csvPrinter.printRecord(headerRow);

            List<String> codesUsed = new ArrayList<>();
//            SortedMap<String, VillageComplex> dorpenCollected = new TreeMap<>();

            int number_of_records_with_multiple_links = 0;
            Set<String> records_to_alter = new TreeSet<>();

            for (Map.Entry<String, Record> record : records.entrySet()) {
                if (record.getValue().links.size() > 1) {
                    number_of_records_with_multiple_links++;
                }
                for (Map.Entry<String, Set<String>> code_hierarchy : codeHierarchy.entrySet()) {
                    if (record.getValue().links.containsAll(code_hierarchy.getValue())) {
                        records_to_alter.add(record.getValue().id);
                    } else if (record.getValue().links.contains(code_hierarchy.getKey())) {
                        records_to_alter.add(record.getValue().id);
                    }
                }
            }

            Set<String> link_codes_not_to_combine = new TreeSet<>();
            for (Map.Entry<String, Set<String>> parentEntry : codeHierarchy.entrySet()) {
                if (parentEntry.getKey().length() == 6) {
                    Set<String> ids = new HashSet<>();
                    for (String child : parentEntry.getValue()) {
                        if (codesToIds.get(child) != null)
                            ids.addAll(codesToIds.get(child));
                    }
                    if (ids.size() > years_from_data.size()) {
                        link_codes_not_to_combine.add(parentEntry.getKey());
                    }
                }
            }

            // Code to make sure some record link codes are not combined
            Map<String, Set<String>> hier_to_check = new HashMap<>();
//            Map<String, Set<String>> hier_to_split = new HashMap<>();
            for (Map.Entry<String, Set<String>> hier_entry : codeHierarchy.entrySet()) {
                Set<String> codes = new TreeSet<>();
                for (Map.Entry<String, Set<String>> code_entry : codesToIds.entrySet()) {
                    for (String hier : hier_entry.getValue()) {
                        if (code_entry.getKey().equals(hier)) {
                            codes.addAll(code_entry.getValue());
                        }
                    }
                }
                if (codes.size() == years_from_data.size()) {
                    hier_to_check.put(hier_entry.getKey(), codes);
//                } else {
//                    hier_to_split.put(hier_entry.getKey(), codes);
                }
            }

            // Checking whether to remove the entries or not, saving them in a separate list
            Set<String> entries_to_remove = new TreeSet<>();
            entryLoop:
            for (Map.Entry<String, Set<String>> entry : hier_to_check.entrySet()) {
                for (String record_id : entry.getValue()) {
                    for (Record rec : records.values()) {
                        if (rec.id.equals(record_id)) {
                            if (!rec.links.containsAll(codeHierarchy.get(entry.getKey()))) {
                                entries_to_remove.add(entry.getKey());
                                continue entryLoop;
                            }
                        }
                    }
                }
            }
            // Removing the entries to remove, due to incomplete children Link Codes
            for (String entry_to_remove : entries_to_remove) {
                hier_to_check.remove(entry_to_remove);
            }

            // Combining link codes if all records contain link codes.
            for (Map.Entry<String, Set<String>> code_to_id : codesToIds.entrySet()) {
                if (records_to_alter.containsAll(code_to_id.getValue())) { // Checks if all the ids for a HO code are present E.G. HO0061B=[279, 1367, 601]
                    for (Record record : records.values()) {
                        if (code_to_id.getValue().contains(record.id)) { // Checks if the id of the record contains one of the ids for code_to_id E.G. 279
                            if (codeHierarchy.entrySet().stream().anyMatch(map -> map.getValue().contains(code_to_id.getKey()))) { // Checks if one of the codehierachies contains the HO code of the code_to_id E.G. HO0061B
                                for (Map.Entry<String, Set<String>> hier_entry : codeHierarchy.entrySet()) {
                                    if (hier_entry.getValue().contains(code_to_id.getKey())) { // Checks if the hierarchy_entry value contains the code_to_id key E.G. HO0061B (See above)
                                        if (record.links.containsAll(hier_entry.getValue())) { // Checks if the links contains all the hierarchy_entry values E.G. HO0061=[HO0061A, HO0061B]
                                            if (link_codes_not_to_combine.contains(hier_entry.getKey()))
                                                continue;
                                            if (!hier_to_check.keySet().contains(hier_entry.getKey()))
                                                continue;
                                            // If all the previous is correct the link codes are adjusted accordingly
                                            String parent = "";
                                            boolean record_is_invalid = false;
                                            for (String s : hier_entry.getValue()) {
                                                if (!record.links.contains(s)) {
                                                    record_is_invalid = true;
                                                    break;
                                                }
                                            }
                                            if (!record_is_invalid) {
                                                for (String s : hier_entry.getValue()) {
                                                    record.links.remove(s);
                                                }
                                                for (Map.Entry<String, Set<String>> code_hierarchy_entry : codeHierarchy.entrySet()) {
                                                    if (code_hierarchy_entry.getValue().contains(code_to_id.getKey())) {
                                                        parent = code_hierarchy_entry.getKey();
                                                    }
                                                }
                                                if (!parent.equals("") && !record.links.contains(parent)) {
                                                    record.links.add(parent);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            //NOTE 23-04-2018 split the record links until the smallest available child(ren)
            //NOTE Commented out for now, as it seemed to break the application
//            for(Record record : records.values()){
//                for(Map.Entry<String, Set<String>> code_hier : codeHierarchy.entrySet()){
//                    if(record.links.contains(code_hier.getKey())){
//                        record.links.remove(code_hier.getKey());
//                        record.links.addAll(code_hier.getValue());
//                    }
//                }
//            }
//


            // Checks for which records the link codes can be split into smaller codes (e.g. their children)
//            for (Map.Entry<String, Set<String>> hier_entry : hier_to_split.entrySet()) {
//                for (String record_id : hier_entry.getValue()) {
//                    for (Record rec : records.values()) {
//                        if (rec.id.equals(record_id)) {
//                            if (rec.links.contains(hier_entry.getKey())) {
//                                if (hier_entry.getValue().size() > 1) {
//                                    Set<String> children = codeHierarchy.get(hier_entry.getKey());
//                                    rec.links.remove(hier_entry.getKey());
//                                    rec.links.addAll(children);
//                                }
//                            }
//                        }
//                    }
//                }
//            }

            // Updating the lists so there are no wrong calculations
            codeHierarchy.clear();
            codesToIds.clear();
            updateLinkRelations();
            updateLinks();

            Collections.sort(codes);

            boolean tried_with_number_of_homes = false;
            while (number_of_records_with_multiple_links != 0) {

                for (Map.Entry<String, Record> record : records.entrySet()) {
                    if (record.getValue().links.size() == 2) {
                        Map<String, Set<String>> code_map = new TreeMap<>();
                        fillCodeMapBasedOnCodeToIds(record, code_map);

                        // Checking if the code_map contains all links of one parent before combining them
                        determineCodesToRemoveAndToAppend(record, code_map);

                        // Gets the unique values and the duplicate values
                        Set<String> duplicates = null;
                        Set<String> uniques = null;
                        duplicates = collectDuplicateLinkCodes(duplicates, code_map);
                        uniques = collectUniquesLinkCodes(uniques, code_map);
                        uniques.removeAll(duplicates);

                        // Removes the unique codes for where the record doesn't contain the specific Link code,
                        // so it doesn't use a child code for a parent or vice verse
                        removeIncompleteChildLinkCodes(record, uniques);

                        // Looping through the unique codes to determine duplicate years in the Link codes
                        TreeSet<Integer> years = new TreeSet<>();

                        Map<String, BigDecimal> uniqueValuesToCalculateFrom = new HashMap<>();
                        determineUniqueValuesToCalculateFrom(uniques, years, uniqueValuesToCalculateFrom);

                        // Determine the value to calculate. These are the codes that are the same, ergo: the codes that need to be split up.
                        Map<String, BigDecimal> equalValueToCalculate = new HashMap<>();
                        for (String duplicate : duplicates) {
                            for (Map.Entry<String, Record> duplicate_record : records.entrySet()) {
                                if (duplicate_record.getValue().id.equals(duplicate)) {
                                    equalValueToCalculate.put(duplicate, duplicate_record.getValue().houses);
                                }
                            }
                        }

                        Integer year_diff = Integer.MAX_VALUE;
                        Integer closest_year_for_calculating_number_of_homes = 0;

                        // Loop through the map containing the values to calculate
                        // Looping through the records to check which record corresponds with the valueToCalculate ID
                        closest_year_for_calculating_number_of_homes = determineClosestYearForCalculatingNumberOfHomes(years, equalValueToCalculate, year_diff, closest_year_for_calculating_number_of_homes);

                        Map<BigDecimal, List<String>> valuesToCalculateWithMap = new HashMap<>();
                        // Loop through the Map of unique values to perform the calculation
                        for (Map.Entry<String, BigDecimal> uniqueValue : uniqueValuesToCalculateFrom.entrySet()) {
                            // Loop through the records to get the record for which the id and year are the same as the given unique value
                            for (Map.Entry<String, Record> unique_value_record : records.entrySet()) {
                                if (unique_value_record.getValue().id.equals(uniqueValue.getKey()) && unique_value_record.getValue().year == closest_year_for_calculating_number_of_homes) {
                                    if (uniqueValue.getValue() != null) {
                                        if (uniqueValue.getValue().compareTo(BigDecimal.ZERO) != 0) {
                                            valuesToCalculateWithMap.put(uniqueValue.getValue(), unique_value_record.getValue().links);
                                        }
                                    }
                                }
                            }
                        }

                        if (valuesToCalculateWithMap.keySet().contains(BigDecimal.ZERO)) {
                            continue;
                        }

                        // Check if the Map with unique values is bigger than 0
                        if (valuesToCalculateWithMap.size() == 2) {
                            List<BigDecimal> values = new ArrayList<>();
                            // Sort the values so the first value is the smallest one for calculation purposes
                            for (Map.Entry<BigDecimal, List<String>> value_to_calculate : valuesToCalculateWithMap.entrySet()) {
                                values.add(value_to_calculate.getKey());
                            }
                            Collections.sort(values);

                            // Loop through the values to calculate in order to get the correct number of houses based on the proportions
                            for (Map.Entry<String, BigDecimal> valueToCalculate : equalValueToCalculate.entrySet()) {
                                Pair result;
                                try {
                                    result = getNumberOfHousesInAccordanceToUpcomingYear(new Pair(values.get(0), values.get(1)), valueToCalculate.getValue());
                                } catch (Exception ex) {
                                    result = new Pair(new BigDecimal(0), new BigDecimal(0));
                                }

                                if (result.lowestNumber.compareTo(BigDecimal.ZERO) != 0 || result.highestNumber.compareTo(BigDecimal.ZERO) != 0) {

                                    // Determine the highest number of the returned value after the calculation
                                    Map<List<String>, BigDecimal> resultMap = new HashMap<>();
                                    resultMap.put(valuesToCalculateWithMap.get(values.get(0)), result.lowestNumber);
                                    resultMap.put(valuesToCalculateWithMap.get(values.get(1)), result.highestNumber);

                                    if (valueToCalculate.getKey().equals(record.getKey())) {
                                        BigDecimal temp = new BigDecimal(0);
                                        for (String record_link : record.getValue().links) {
                                            Record r = new Record();
                                            r.year = record.getValue().year;
                                            r.links.add(record_link);
                                            r.yearUsedToCalculate = closest_year_for_calculating_number_of_homes;
                                            r.note = NoteState.YEAR_SOURCE;

                                            for (Map.Entry<List<String>, BigDecimal> entry : resultMap.entrySet()) {
                                                if (entry.getKey().contains(record_link)) {
                                                    r.houses = entry.getValue();
                                                }
                                            }
                                            temp = temp.add(r.houses);

                                            r.id = record.getKey() + record.getValue().year + r.links.toString() + r.houses;
                                            if (!recordsToAdd.containsKey(r.id)) {
                                                recordsToAdd.put(r.id, r);
                                            }
                                        }
                                        recordsToRemove.add(valueToCalculate.getKey());
                                    }
                                } else if (record.getValue().houses != null && record.getValue().houses.equals(BigDecimal.ZERO)) {
                                    for (String link : record.getValue().links) {
                                        Record r = new Record();
                                        r.year = record.getValue().year;
                                        r.links.add(link);
                                        r.houses = record.getValue().houses;
                                        r.yearUsedToCalculate = closest_year_for_calculating_number_of_homes;
                                        r.note = NoteState.YEAR_SOURCE;
                                        r.id = record.getKey() + record.getValue().year + r.links.toString() + r.houses;
                                        if (!recordsToAdd.containsKey(r.id)) {
                                            recordsToAdd.put(r.id, r);
                                        }
                                    }
                                    recordsToRemove.add(record.getValue().id);
                                } else if (record.getValue().houses == null) {
                                    splitRecordWithNullHomes(record);
                                    recordsToRemove.add(record.getValue().id);
                                }
                            }
                        } else if ((valuesToCalculateWithMap.size() <= 1 || valuesToCalculateWithMap.size() > 2) && tried_with_number_of_homes) {
                            Map<String, BigDecimal> squareKilometresToCalculateWithMap = new HashMap<>();
                            BigDecimal totalSquareKilometres = new BigDecimal(0);
                            totalSquareKilometres = collectSquareKmsToCalculateWith(record, code_map, squareKilometresToCalculateWithMap, totalSquareKilometres);
                            if (squareKilometresToCalculateWithMap.size() > 0 && !totalSquareKilometres.equals(new BigDecimal(0))) {
                                for (Map.Entry<String, BigDecimal> entry : squareKilometresToCalculateWithMap.entrySet()) {
                                    BigDecimal ratio = entry.getValue().divide(totalSquareKilometres, BigDecimal.ROUND_HALF_EVEN);
                                    BigDecimal numberOfHomes;
                                    if (record.getValue().houses != null)
                                        numberOfHomes = record.getValue().houses.multiply(ratio).setScale(3, BigDecimal.ROUND_HALF_EVEN);
                                    else
                                        numberOfHomes = BigDecimal.ZERO;
                                    Record newRecord = new Record();
                                    newRecord.year = record.getValue().year;
                                    newRecord.links.add(entry.getKey());
                                    newRecord.km2 = entry.getValue();
                                    newRecord.houses = numberOfHomes;
                                    newRecord.yearUsedToCalculate = record.getValue().year;
                                    newRecord.note = NoteState.YEAR_SURFACE;

                                    newRecord.id = record.getKey() + record.getValue().year + entry.getKey() + newRecord.houses;

                                    if (!recordsToAdd.containsValue(newRecord)) {
                                        recordsToAdd.put(newRecord.id, newRecord);
                                    }
                                }
                                recordsToRemove.add(record.getKey());
                                tried_with_number_of_homes = false;
                                break;
                            }
                        } else if (valuesToCalculateWithMap.size() == 1) {
                            List<Record> recordsToUseForCalculation = new ArrayList<>();

                            for (Record r : records.values()) {
                                for (Map.Entry<String, BigDecimal> entry : uniqueValuesToCalculateFrom.entrySet()) {
                                    if (r.id.equals(entry.getKey()) && r.year == closest_year_for_calculating_number_of_homes) {
                                        recordsToUseForCalculation.add(r);
                                    }
                                }
                            }

                            if (recordsToUseForCalculation.size() == 2) {
                                Pair result;
                                try {
                                    result = getNumberOfHousesInAccordanceToUpcomingYear(new Pair(recordsToUseForCalculation.get(0).houses, recordsToUseForCalculation.get(1).houses), record.getValue().houses);
                                } catch (Exception ex) {
                                    result = new Pair(new BigDecimal(0), new BigDecimal(0));
                                }

                                // Determine the highest number of the returned value after the calculation
                                Map<List<String>, BigDecimal> resultMap = new HashMap<>();
                                resultMap.put(valuesToCalculateWithMap.get(recordsToUseForCalculation.get(0).houses), result.lowestNumber);
                                resultMap.put(valuesToCalculateWithMap.get(recordsToUseForCalculation.get(1).houses), result.highestNumber);

                                if (record.getValue().id.equals(record.getKey())) {
                                    BigDecimal temp = new BigDecimal(0);
                                    for (String record_link : record.getValue().links) {
                                        Record r = new Record();
                                        r.year = record.getValue().year;
                                        r.links.add(record_link);
                                        r.yearUsedToCalculate = closest_year_for_calculating_number_of_homes;
                                        r.note = NoteState.YEAR_SOURCE;

                                        for (Map.Entry<List<String>, BigDecimal> entry : resultMap.entrySet()) {
                                            try {
                                                if (entry.getKey().contains(record_link)) {
                                                    r.houses = entry.getValue();
                                                    break;
                                                }else{
                                                    r.houses = null;
                                                }
                                            } catch (NullPointerException ex) {
                                                r.houses = entry.getValue();
                                            }
                                        }
                                        temp = r.houses == null ? BigDecimal.ZERO : temp.add(r.houses);

                                        r.id = record.getKey() + record.getValue().year + r.links.toString() + r.houses;
                                        if (!recordsToAdd.containsKey(r.id)) {
                                            recordsToAdd.put(r.id, r);
                                        }
                                    }
                                    recordsToRemove.add(record.getValue().id);
                                }
                            }
                        }
                    } else if (record.getValue().links.size() > 2) {
                        Map<String, Set<String>> code_map = new TreeMap<>();
                        fillCodeMapBasedOnCodeToIds(record, code_map);

                        determineCodesToRemoveAndToAppend(record, code_map);

                        // Gets the unique values and the duplicate values
                        Set<String> duplicates = null;
                        Set<String> uniques = null;
                        duplicates = collectDuplicateLinkCodes(duplicates, code_map);
                        uniques = collectUniquesLinkCodes(uniques, code_map);
                        uniques.removeAll(duplicates);

                        // Removes the unique codes for where the record doesn't contain the specific Link code,
                        // so it doesn't use a child code for a parent or vice verse
                        removeIncompleteChildLinkCodes(record, uniques);
                        Boolean do_links_compare = true;
                        List<Record> recordList = new ArrayList<>();
                        for (String duplicate : duplicates) {
                            for (Record rec : records.values()) {
                                if (rec.id.equals(duplicate)) {
                                    recordList.add(rec);
                                }
                            }
                        }

                        for (Record rec : recordList) {
                            for (Record rec2 : recordList) {
                                if (!rec.id.equals(rec2.id)) {
                                    if (!rec.links.equals(rec2.links)) {
                                        do_links_compare = false;
                                    }
                                }
                            }
                        }
                        if (recordList.size() == 1)
                            do_links_compare = false;

                        if (uniques.size() >= record.getValue().links.size() && do_links_compare) {
                            // Looping through the unique codes to determine duplicate years in the Link codes
                            TreeSet<Integer> years = new TreeSet<>();

                            Map<String, BigDecimal> uniqueValuesToCalculateFrom = new HashMap<>();
                            determineUniqueValuesToCalculateFrom(uniques, years, uniqueValuesToCalculateFrom);


                            // Determine the value to calculate. These are the codes that are the same, ergo: the codes that need to be split up.
                            Map<String, BigDecimal> equalValueToCalculate = new HashMap<>();
                            for (String duplicate : duplicates) {
                                for (Map.Entry<String, Record> duplicate_record : records.entrySet()) {
                                    if (duplicate_record.getValue().id.equals(duplicate)) {
                                        equalValueToCalculate.put(duplicate, duplicate_record.getValue().houses);
                                    }
                                }
                            }

                            Integer year_diff = Integer.MAX_VALUE;
                            Integer closest_year_for_calculating_number_of_homes = 0;

                            // Loop through the map containing the values to calculate
                            // Looping through the records to check which record corresponds with the valueToCalculate ID
                            closest_year_for_calculating_number_of_homes = determineClosestYearForCalculatingNumberOfHomes(years, equalValueToCalculate, year_diff, closest_year_for_calculating_number_of_homes);

                            Map<BigDecimal, List<String>> valuesToCalculateWithMap = new HashMap<>();
                            // Loop through the Map of unique values to perform the calculation
                            for (Map.Entry<String, BigDecimal> uniqueValue : uniqueValuesToCalculateFrom.entrySet()) {
                                // Loop through the records to get the record for which the id and year are the same as the given unique value
                                for (Map.Entry<String, Record> unique_value_record : records.entrySet()) {
                                    if (unique_value_record.getValue().id.equals(uniqueValue.getKey()) && unique_value_record.getValue().year == closest_year_for_calculating_number_of_homes) {
                                        valuesToCalculateWithMap.put(uniqueValue.getValue(), unique_value_record.getValue().links);
                                    }
                                }
                            }

                            BigDecimal total_to_calculate_from = new BigDecimal(0);
                            for (BigDecimal bd : valuesToCalculateWithMap.keySet()) {
                                total_to_calculate_from = total_to_calculate_from.add(bd);
                            }

                            if (total_to_calculate_from.compareTo(BigDecimal.ZERO) != 0) {
                                for (Map.Entry<String, BigDecimal> entry_to_recalculate : equalValueToCalculate.entrySet()) {
                                    Map<BigDecimal, List<String>> calculated_home_values = new HashMap<>();
                                    for (Map.Entry<BigDecimal, List<String>> entry : valuesToCalculateWithMap.entrySet()) {
                                        BigDecimal ratio = entry.getKey().divide(total_to_calculate_from, 9, BigDecimal.ROUND_HALF_EVEN);
                                        BigDecimal result;
                                        if (entry_to_recalculate.getValue() != null)
                                            result = ratio.multiply(entry_to_recalculate.getValue());
                                        else
                                            result = new BigDecimal(0);
                                        calculated_home_values.put(result, entry.getValue());
                                    }

                                    BigDecimal number_of_homes_validator = new BigDecimal(0);
                                    for (Map.Entry<BigDecimal, List<String>> calculated_home_value : calculated_home_values.entrySet()) {
                                        Record r = new Record();
                                        r.houses = calculated_home_value.getKey().setScale(3, BigDecimal.ROUND_HALF_EVEN);
                                        r.year = records.get(entry_to_recalculate.getKey()).year;
                                        r.links = calculated_home_value.getValue();
                                        r.yearUsedToCalculate = closest_year_for_calculating_number_of_homes;
                                        r.note = NoteState.YEAR_SOURCE;
                                        r.id = entry_to_recalculate.getKey() + r.year + r.links.toString() + r.houses;
                                        number_of_homes_validator = number_of_homes_validator.add(r.houses);

                                        if (!recordsToAdd.containsKey(r.id)) {
                                            recordsToAdd.put(r.id, r);
                                        }
                                    }
                                    recordsToRemove.add(entry_to_recalculate.getKey());
                                }
                            }
                        } else { // use square kilometres to calculate the number of homes
                            if(tried_with_number_of_homes) { // Check to make the use of square kilometres less frequent
                                if (record.getValue().houses != null) {
                                    Map<String, BigDecimal> squareKilometresToCalculateWithMap = new HashMap<>();
                                    BigDecimal totalSquareKilometres = new BigDecimal(0);
                                    totalSquareKilometres = collectSquareKmsToCalculateWith(record, code_map, squareKilometresToCalculateWithMap, totalSquareKilometres);
                                    if (squareKilometresToCalculateWithMap.size() > 0 && !totalSquareKilometres.equals(new BigDecimal(0))) {
                                        for (Map.Entry<String, BigDecimal> entry : squareKilometresToCalculateWithMap.entrySet()) {
                                            BigDecimal ratio = entry.getValue().divide(totalSquareKilometres, BigDecimal.ROUND_HALF_EVEN);
                                            BigDecimal numberOfHomes = record.getValue().houses.multiply(ratio).setScale(3, BigDecimal.ROUND_HALF_EVEN);
                                            Record newRecord = new Record();
                                            newRecord.year = record.getValue().year;
                                            newRecord.links.add(entry.getKey());
                                            newRecord.km2 = entry.getValue();
                                            newRecord.houses = numberOfHomes;
                                            newRecord.yearUsedToCalculate = record.getValue().year;
                                            newRecord.note = NoteState.YEAR_SURFACE;

                                            newRecord.id = record.getKey() + record.getValue().year + entry.getKey() + newRecord.houses;

                                            if (!recordsToAdd.containsValue(newRecord)) {
                                                recordsToAdd.put(newRecord.id, newRecord);
                                            }
                                        }
                                        recordsToRemove.add(record.getKey());
                                        break;
                                    }
                                } else {
                                    splitRecordWithNullHomes(record);
                                    recordsToRemove.add(record.getValue().id);
                                }
                            }
                        }
                    }
                }

                for (String id : recordsToRemove) {
                    records.remove(id);
                }
                recordsToRemove.clear();

                for (Map.Entry<String, Record> record_to_add : recordsToAdd.entrySet()) {
                    record_to_add.getValue().id = Integer.toString(record_id_counter);
                    records.put(Integer.toString(record_id_counter), record_to_add.getValue());
                    record_id_counter++;
                }
                recordsToAdd.clear();

                BigDecimal number_of_homes = new BigDecimal(0);
                for (Record record : records.values()) {
                    number_of_homes = record.houses != null ? number_of_homes.add(record.houses) : number_of_homes.add(new BigDecimal(0));
                }

                int duplicate_link_code_validator = 0;
                for (Record record : records.values()) {
                    if (record.links.size() > 1) {
                        duplicate_link_code_validator++;
                    }
                }

                if (duplicate_link_code_validator == number_of_records_with_multiple_links) {
                    if (tried_with_number_of_homes)
                        break;
                    else {
                        tried_with_number_of_homes = true;
                    }
                } else {
                    number_of_records_with_multiple_links = duplicate_link_code_validator;
                    codeHierarchy.clear();
                    codesToIds.clear();
                    updateLinkRelations();
                    updateLinks();
                }
            }

            // ################################################################################# //
            // Making sure the records have link codes as small as possible.
            // Whilst recalculating the number of homes per record.
            Map<Record, Set<String>> recordsToSplitToSmallerLinks = new HashMap<>();
            for (Map.Entry<String, Set<String>> code_hier_entry : codeHierarchy.entrySet()) {
                for (Record record : records.values()) {
                    if (record.links.contains(code_hier_entry.getKey())) {
                        if (code_hier_entry.getValue().size() > 1) {
                            Set<String> codes = new TreeSet<>();
                            try {
                                for (String child : code_hier_entry.getValue()) {
                                    if (codeHierarchy.get(child).size() > 1) {
                                        codes.addAll(codeHierarchy.get(child));
                                    }
                                }
                            } catch (NullPointerException ex) {
                                codes.addAll(code_hier_entry.getValue());
                            }
                            recordsToSplitToSmallerLinks.put(record, codes);
                        }
                    }
                }
            }

            int year_of_record_to_split_the_current_record_with = 0;
            Map<String, List<Record>> recordsToSplitWith = new HashMap<>();
            for (Map.Entry<Record, Set<String>> record_to_split : recordsToSplitToSmallerLinks.entrySet()) {
                List<Record> recordsToAdd = new ArrayList<>();
                for (Record record : records.values()) {
                    if (record_to_split.getValue().contains(record.links.get(0)) || record_to_split.getValue().contains(record.links.get(0))) {
                        recordsToAdd.add(record);
                        year_of_record_to_split_the_current_record_with = record.year;
                    }
                }
                recordsToSplitWith.put(record_to_split.getKey().links.get(0), recordsToAdd);
            }

            List<Record> recordsToRemoveFromDataset = new ArrayList<>();
            List<Record> recordsToAddToDataset = new ArrayList<>();
            for (Map.Entry<String, List<Record>> recordEntry : recordsToSplitWith.entrySet()) {
                BigDecimal totalNumber = new BigDecimal(0);
                for (Record record_to_collect_homes : recordEntry.getValue()) {
                    totalNumber = totalNumber.add(record_to_collect_homes.houses);
                }
                for (Record record_to_calculate_with : recordEntry.getValue()) {
                    BigDecimal ratio = record_to_calculate_with.houses.divide(totalNumber, 3, BigDecimal.ROUND_HALF_EVEN);
                    for (Map.Entry<Record, Set<String>> recordsToSplit : recordsToSplitToSmallerLinks.entrySet()) {
                        if (recordsToSplit.getValue().contains(record_to_calculate_with.links.get(0)) || recordsToSplit.getValue().contains(record_to_calculate_with.links.get(0).substring(0, record_to_calculate_with.links.get(0).length()))) {
                            BigDecimal result;
                            if(recordsToSplit.getKey().houses == null){
                                result = null;
                            }else{
                                try {
                                    result = ratio.multiply(recordsToSplit.getKey().houses);
                                }catch(NullPointerException ex){
                                    result = BigDecimal.ZERO;
                                }
                            }
                            Record newRecord = new Record();
                            newRecord.year = recordsToSplit.getKey().year;
                            newRecord.links.addAll(record_to_calculate_with.links);
                            newRecord.km2 = null;
                            newRecord.houses = result;
                            newRecord.yearUsedToCalculate = recordsToSplit.getKey().yearUsedToCalculate;
                            newRecord.note = recordsToSplit.getKey().note;
                            recordsToAddToDataset.add(newRecord);
                        }
                        if (!recordsToRemoveFromDataset.contains(recordsToSplit.getKey())) {
                            recordsToRemoveFromDataset.add(recordsToSplit.getKey());
                        }
                    }
                }
            }

            for (Record record : recordsToRemoveFromDataset) {
                records.remove(record.id);
            }

            for (Record record : recordsToAddToDataset) {
                record.id = Integer.toString(record_id_counter);
                records.put(Integer.toString(record_id_counter), record);
                record_id_counter++;
            }
            // End of splitting the record links from parents to children
            // ################################################################################# //

            BigDecimal number_of_homes = new BigDecimal(0);
            for (Record record : records.values()) {
                number_of_homes = record.houses != null ? number_of_homes.add(record.houses) : number_of_homes.add(BigDecimal.ZERO);
            }
            System.out.println("Final number of duplicate links is: " + number_of_records_with_multiple_links);
            System.out.println("Final number of homes is: " + number_of_homes);

            // Looping through the link codes combined with the ids belonging to the code
            for (String entry : codes) {
                VillageComplex village = new VillageComplex();

                // Adding the Link codes to a list for further processing, removing duplicates
                // And adding the Link to the dorpenComplex map for processing
                if (!codesUsed.contains(entry)) {
                    if (records.values().stream().anyMatch(map -> map.links.contains(entry))) {
                        codesUsed.add(entry);
                        SortedSet<String> other_link_codes = new TreeSet<>();
                        other_link_codes.add(entry);
                        village.linkCode = new Tuple(entry, "0", NoteState.SOURCE, other_link_codes, 0);
                    }
                }

                // Checking to see if the dorpencomplex Hashmap is empty
                if (village.linkCode != null) {
                    // Looping through the records collected from the CSV file
                    for (Map.Entry<String, Record> recordEntry : records.entrySet()) {
                        // Check if the record entry attribute 'links' contains the Link code from dorpencomplex Hashmap
                        // If so, it adds the place of the record entry in the dorpencomplex Hashmap
                        for (String link : recordEntry.getValue().links) {
                            if (link.equals(village.linkCode.key)) {
                                // Tries to put the year belonging to the record in the Hashmap dorpenComplex
                                try {
                                    if (recordEntry.getValue().links.size() == 1) {
                                        village.linkCode.otherLinkCodes.addAll(recordEntry.getValue().links);
                                        SortedSet<String> set = new TreeSet<>(recordEntry.getValue().links);
                                        village.yearMap.put(Integer.toString(recordEntry.getValue().year), new Tuple(recordEntry.getValue().houses != null ? recordEntry.getValue().houses.toString() : "N/A", recordEntry.getValue().id, recordEntry.getValue().note, set, recordEntry.getValue().yearUsedToCalculate));
                                    } else {
                                        village.linkCode.otherLinkCodes.addAll(recordEntry.getValue().links);
                                        SortedSet<String> set = new TreeSet<>(recordEntry.getValue().links);
                                        village.yearMap.put(Integer.toString(recordEntry.getValue().year), new Tuple(recordEntry.getValue().houses != null ? recordEntry.getValue().houses.toString() : "N/A", recordEntry.getValue().id, recordEntry.getValue().note, set, recordEntry.getValue().yearUsedToCalculate));
                                    }
                                    break;
                                } catch (NullPointerException e) {
                                    System.out.println("NULLPOINTER!!");
                                }
                            }
                        }
                    }

                    // Checks if the length of the Link Code is longer than 6 characters
                    // If so, it gets the code from the codeHierarchy, by checking if the codehierarchy code equals the beginning of the dorpencomplex code
                    dorpenCollected.put(village.linkCode.key.toString(), village);
                }
            }

//            // NOTE using for testing purposes
//            System.out.println(getLineNumber() + " -> Printing the values to houses export.csv");
//            for (Integer year : years_from_data) {
//                for (Record record : records.values()) {
//                    if (record.year == year) {
//                        List<String> dorpenOutput = new ArrayList<>();
//                        for (String s : headerRow) {
//                            try {
//                                switch (s) {
//                                    case "YEAR":
//                                        dorpenOutput.add(Integer.toString(record.year));
//                                        break;
//                                    case "LINK":
//                                        dorpenOutput.add(record.links.toString());
//                                        break;
//                                    case "HOUSES":
//                                        dorpenOutput.add(record.houses.toString());
//                                        break;
//                                    case "KM2":
//                                        dorpenOutput.add(record.km2.toString());
//                                        break;
//                                    default:
//                                        break;
//                                }
//                            } catch (Exception e) {
//                                dorpenOutput.add("N/A");
//                            }
//                        }
//                        csvPrinter.printRecord(dorpenOutput);
//                    }
//                }
//            }
//            csvPrinter.flush();
//            csvPrinter.close();

            // Converts the dorpenCollected so it can be saved in the CSV file!
            for (Map.Entry<String, VillageComplex> dorpCollected : dorpenCollected.entrySet()) {
                List<String> dorpenOutput = new ArrayList<>();
                for (String s : headerRow) {
                    try {
                        switch (s) {
                            case "Code":
                                dorpenOutput.add(dorpCollected.getValue().linkCode.key.toString());
                                break;
                            default:
                                if(dorpCollected.getValue().yearMap.get(s).key != null && dorpCollected.getValue().yearMap.get(s).key != "N/A") {
                                    if (new BigDecimal(dorpCollected.getValue().yearMap.get(s).key.toString()).compareTo(BigDecimal.ZERO) == 0) {
                                        dorpenOutput.add("0");
                                    } else {
                                        BigDecimal numberOfHomes = new BigDecimal(dorpCollected.getValue().yearMap.get(s).key.toString()).setScale(3, BigDecimal.ROUND_HALF_EVEN);
                                        dorpenOutput.add(numberOfHomes.toString());
                                    }
                                }else{
                                    dorpenOutput.add("N/A");
                                }
                                break;
                        }
                    } catch (NullPointerException e) {
                        dorpenOutput.add("N/A");
                    }
                }
                csvPrinter.printRecord(dorpenOutput);
            }
            csvPrinter.flush();
            csvPrinter.close();
        }

        // Prints the notes data to a separate CSV file.
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(notesPath))) {
            CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat);

            List<String> headerRow = new ArrayList<>();
            headerRow.add("Code");
            @SuppressWarnings("ComparatorMethodParameterNotUsed")
            List<String> temp_list = years_from_data.stream().sorted((t1, t2) -> (t1 <= t2) ? -1 : 1).map(map -> Integer.toString(map)).collect(Collectors.toList());
            headerRow.addAll(temp_list);

            csvPrinter.printRecord(headerRow);

            // Converts the dorpenCollected so it can be saved in the CSV file!
            for (Map.Entry<String, VillageComplex> dorpCollected : dorpenCollected.entrySet()) {
                List<String> dorpenOutput = new ArrayList<>();
                for (String s : headerRow) {
                    try {
                        switch (s) {
                            case "Code":
                                dorpenOutput.add(dorpCollected.getValue().linkCode.key.toString());
                                break;
                            default:
                                dorpenOutput.add(dorpCollected.getValue().yearMap.get(s).noteState.getState(dorpCollected.getValue().yearMap.get(s).otherYear));
                                break;
                        }
                    } catch (NullPointerException e) {
                        dorpenOutput.add("N/A");
                    }
                }
                csvPrinter.printRecord(dorpenOutput);
            }
            csvPrinter.flush();
            csvPrinter.close();
        }
    }

    private static void splitRecordWithNullHomes(Map.Entry<String, Record> record) {
        for (String link : record.getValue().links) {
            Record r = new Record();
            r.year = record.getValue().year;
            r.links.add(link);
            r.houses = null;
            r.note = NoteState.SOURCE;

            r.id = record.getKey() + record.getValue().year + r.links.toString() + r.houses;
            if (!recordsToAdd.containsKey(r.id)) {
                recordsToAdd.put(r.id, r);
            }
        }
    }

    /**
     * Collects the square kilometres that are needed to calculate the new number of homes for the specific record.
     *
     * @param record                             Map.Entry<String, Record> contains the record entry to be checked from the list of records.
     * @param code_map                           Map<String, Set<String>> contains the ids belonging to a specific Link Code
     * @param squareKilometresToCalculateWithMap Map<String, BigDecimal> the map in which the square kilometres will be put along with the Link Code
     * @param totalSquareKilometres              BigDecimal in which the total of square kilometres for a specific Link Code is held.
     * @return BigDecimal
     */
    private static BigDecimal collectSquareKmsToCalculateWith(Map.Entry<String, Record> record, Map<String, Set<String>> code_map, Map<String, BigDecimal> squareKilometresToCalculateWithMap, BigDecimal totalSquareKilometres) {
        for (Map.Entry<String, Set<String>> code_to_id : code_map.entrySet()) {
            for (SquareKilometreRecord srecord : squareKilometreRecords) {
                if (srecord.linkCode.equals(code_to_id.getKey())) {
                    if (srecord.km2.get(record.getValue().year) != null) {
                        squareKilometresToCalculateWithMap.put(code_to_id.getKey(), srecord.km2.get(record.getValue().year));
                        totalSquareKilometres = totalSquareKilometres.add(srecord.km2.get(record.getValue().year));
                    }
                }
            }
        }
        return totalSquareKilometres;
    }

    /**
     * Determines the closest year with which the new number of homes will be calculated with.
     *
     * @param years                          TreeSet<Integer> holds the years belonging to the Link Codes
     * @param equalValueToCalculate          Map<String, BigDecimal> contains the values to calculate which have equal Link Codes.
     * @param year_diff                      Integer holds the difference of the closest years, needed to determine the closest year
     * @param closest_year_to_calculate_from Integer will hold the closest year to calculate from
     * @return Integer the closest year to calculate the new number of homes
     */
    private static Integer determineClosestYearForCalculatingNumberOfHomes(TreeSet<Integer> years, Map<String, BigDecimal> equalValueToCalculate, Integer year_diff, Integer closest_year_to_calculate_from) {
        Integer yearToCheck;
        for (Map.Entry<String, Record> calculate_record : records.entrySet()) {
            // Get the year to perform the check upon */
            if (equalValueToCalculate.containsKey(calculate_record.getValue().id)) {
                yearToCheck = calculate_record.getValue().year;

                // Determine which year is closest to the year to calculate the values for
                for (Integer yearToCalculateFrom : years) {
                    Integer timeBetweenYears = yearToCalculateFrom - yearToCheck;
                    if (timeBetweenYears < 0) {
                        timeBetweenYears = timeBetweenYears * -1;
                    }
                    if (timeBetweenYears < year_diff) {
                        closest_year_to_calculate_from = yearToCalculateFrom;
                        year_diff = timeBetweenYears;
                    }
                }
            }

        }
        return closest_year_to_calculate_from;
    }

    /**
     * Determines the unique values to calculate the new number of homes from
     *
     * @param uniques                     Set<String> holds the ids of the unique records, e.g. records with only one Link Code
     * @param years                       TreeSet<Integer> holds the years belonging to the Link Code(s)
     * @param uniqueValuesToCalculateFrom Map<String, BigDecimal> contains the unique values to calculate the new number of homes from.
     */
    private static void determineUniqueValuesToCalculateFrom(Set<String> uniques, TreeSet<Integer> years, Map<String, BigDecimal> uniqueValuesToCalculateFrom) {
        for (String unique_code : uniques) {
            for (Map.Entry<String, Record> unique_record : records.entrySet()) {
                if (unique_record.getValue().id.equals(unique_code)) {
                    uniqueValuesToCalculateFrom.put(unique_code, unique_record.getValue().houses);
                    if (!years.add(unique_record.getValue().year)) {
                        years.add(unique_record.getValue().year);
                    }
                }
            }
        }
    }

    /**
     * Removes ids from the unique Set if the Link Codes for that id don't complete the parent Link Code.
     *
     * @param record  Map.Entry<String, Record> the record to check the link codes from
     * @param uniques Set<String> the Set from which to remove the ids of incomplete Link Codes.
     */
    private static void removeIncompleteChildLinkCodes(Map.Entry<String, Record> record, Set<String> uniques) {
        for (Record record_unique : records.values()) {
            if (uniques.contains(record_unique.id)) {
                for (String s : record_unique.links) {
                    if (!record.getValue().links.contains(s)) {
                        uniques.remove(record_unique.id);
                    }
                }
            }
        }
    }

    /**
     * Collects duplicate Link Codes, E.G. checks which Link Codes contain the same record ids and saves them to the duplicates set
     *
     * @param duplicates Set<String> contains the duplicate ids
     * @param code_map   Map<String, Set<String> contains the ids belonging to a Link Code
     * @return Set<String> which contains the duplicate ids
     */
    private static Set<String> collectDuplicateLinkCodes(Set<String> duplicates, Map<String, Set<String>> code_map) {
        for (String key : code_map.keySet()) {
            if (duplicates == null) {
                duplicates = new TreeSet<>(code_map.get(key));
            } else {
                duplicates.retainAll(code_map.get(key));
            }
        }
        return duplicates;
    }

    /**
     * Collects unique Link Codes, E.G. checks which Link Codes don't contain the same record ids and saves them to the uniques set
     *
     * @param uniques  Set<String> contains the unique ids
     * @param code_map Map<String, Set<String> contains the ids belonging to a Link Code
     * @return Set<String> which contains the unique ids
     */
    private static Set<String> collectUniquesLinkCodes(Set<String> uniques, Map<String, Set<String>> code_map) {
        for (String key : code_map.keySet()) {
            if (uniques == null) {
                uniques = new TreeSet<>(code_map.get(key));
            } else {
                uniques.addAll(code_map.get(key));
            }
        }
        return uniques;
    }

    /**
     * Determines the Link Codes to remove from the code_map and also the Link Codes to add to the code_map.
     * Depending on whether or not the Link Codes are (complete) children of a parent Link Code
     *
     * @param record   Map.Entry<String, Record> the entry to check for if the link codes complete the parent code
     * @param code_map Map<String, Set<String> the map containing the Link Code with the ids that contain that Link Code
     */
    private static void determineCodesToRemoveAndToAppend(Map.Entry<String, Record> record, Map<String, Set<String>> code_map) {
        for (String link : record.getValue().links) {
            Set<String> testSet = codeHierarchy.get(link.substring(0, link.length() - 1)) != null ? codeHierarchy.get(link.substring(0, link.length() - 1)) : new TreeSet<>();
            if (code_map.values().containsAll(testSet)) {

                Map<String, Set<String>> code_map_to_append = new TreeMap<>();
                Set<String> codes_to_remove = new TreeSet<>();
                Map.Entry<String, Set<String>> firstEntry = code_map.entrySet().iterator().next();
                for (Map.Entry<String, Set<String>> next : code_map.entrySet()) {
                    if (!next.equals(firstEntry)) {
                        if (next.getKey().startsWith("HO")) {
                            if (next.getKey().substring(0, 6).equals(firstEntry.getKey().substring(0, 6)) && next.getValue().equals(firstEntry.getValue())) {
                                codes_to_remove.add(firstEntry.getKey());
                                codes_to_remove.add(next.getKey());
                                code_map_to_append.put(firstEntry.getKey().substring(0, 6), firstEntry.getValue());
                            } else {
                                codes_to_remove.clear();
                            }
                        } else {
                            if (next.getKey().equals(firstEntry.getKey()) && next.getValue().equals(firstEntry.getValue())) {
                                codes_to_remove.add(firstEntry.getKey());
                                codes_to_remove.add(next.getKey());
                                code_map_to_append.put(firstEntry.getKey(), firstEntry.getValue());
                            } else {
                                codes_to_remove.clear();
                            }
                        }
                        firstEntry = next;
                    }
                }

                for (String code_to_remove : codes_to_remove) {
                    code_map.remove(code_to_remove);
                }
                code_map.putAll(code_map_to_append);
            }
        }
    }

    /**
     * Fills the code_map based on the codeToIds Map.
     * This by checking whether or not the codeToIds contain the Link code.
     *
     * @param record   Map.Entry<String, Record> the entry to check for in the codeToIds map
     * @param code_map Map<String, Set<String> the map containing the Link Code with the ids that contain that Link Code
     */
    private static void fillCodeMapBasedOnCodeToIds(Map.Entry<String, Record> record, Map<String, Set<String>> code_map) {
        for (String link : record.getValue().links) {
            for (Map.Entry<String, Set<String>> codeToIdEntry : codesToIds.entrySet()) {
                if (codeToIdEntry.getKey().equals(link)) {
                    if (code_map.entrySet().stream().noneMatch(map -> map.getValue().equals(codeToIdEntry))) {
                        code_map.put(codeToIdEntry.getKey(), codeToIdEntry.getValue());
                    }
                } else {
                    List<Set<String>> childLinks = codeHierarchy.entrySet().stream().filter(map -> map.getKey().equals(link)).map(Map.Entry::getValue).collect(Collectors.toList());
                    if (childLinks.size() > 0) {
                        for (String child : childLinks.get(0)) {
                            if (codeToIdEntry.getKey().equals(child)) {
                                if (code_map.entrySet().stream().noneMatch(map -> map.getValue().contains(child)) && codeToIdEntry.getValue().contains(record.getKey())) {
                                    code_map.put(codeToIdEntry.getKey(), codeToIdEntry.getValue());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Updates the relations between the ids and the Link Codes aswell the Parent to Child relation for the Link Codes
     */
    private static void updateLinkRelations() {
        for (Record record : records.values()) {
            for (String code : record.links) {
                Set<String> ids = codesToIds.getOrDefault(code, new HashSet<>());
                ids.add(record.id);
                codesToIds.put(code, ids);

                if (code.contains("HO")) {
                    setParentRelation(code);
                }
            }
        }
    }

    /**
     * Calculates the number of homes for the new records by using the closest year.
     * This only works if the record contains two Link Codes
     *
     * @param upcomingYear          Pair contains the year and the values to calculate from.
     * @param numberOfHousesToSplit BigDecimal contains the number of homes to split for the new records.
     * @return Pair containing the new number of homes
     */
    private static Pair getNumberOfHousesInAccordanceToUpcomingYear(Pair upcomingYear, BigDecimal numberOfHousesToSplit) {
        BigDecimal lowestResult;
        BigDecimal highestResult;
        if(upcomingYear.lowestNumber == null){
            lowestResult = null;
            highestResult = numberOfHousesToSplit;
        }else if(upcomingYear.highestNumber == null){
            lowestResult = numberOfHousesToSplit;
            highestResult = null;
        }else {
            BigDecimal percentage = upcomingYear.lowestNumber.divide(upcomingYear.highestNumber.add(upcomingYear.lowestNumber), 9, BigDecimal.ROUND_HALF_EVEN); //.setScale(9, BigDecimal.ROUND_HALF_EVEN);
            lowestResult = numberOfHousesToSplit.multiply(percentage).setScale(3, BigDecimal.ROUND_HALF_EVEN);
            highestResult = numberOfHousesToSplit.subtract(lowestResult).setScale(3, BigDecimal.ROUND_HALF_EVEN);
        }
        return new Pair(lowestResult, highestResult);
    }

    /**
     * Class to contain multiple values for further calculation
     */
    private static class Tuple {
        Object key;
        String value;
        SortedSet<String> otherLinkCodes;
        NoteState noteState;
        int otherYear;

        /**
         * Constructor for the Tuple class
         *
         * @param key            Object
         * @param value          String
         * @param noteState      NoteState
         * @param otherLinkCodes SortedSet<String>
         */
        Tuple(Object key, String value, NoteState noteState, SortedSet<String> otherLinkCodes, int otherYear) {
            this.key = key;
            this.value = value;
            this.noteState = noteState;
            this.otherLinkCodes = otherLinkCodes;
            this.otherYear = otherYear;
        }
    }

    /**
     * Class to contain the lowest and highest number of homes
     */
    private static class Pair {
        BigDecimal lowestNumber;
        BigDecimal highestNumber;

        /**
         * Constructor for the class Pair
         *
         * @param low  BigDecimal low number of homes
         * @param high BigDecimal high number of homes
         */
        Pair(BigDecimal low, BigDecimal high) {
            lowestNumber = low;
            highestNumber = high;
        }

        /**
         * Returns the lowest and highest number in a String
         *
         * @return String
         */
        public String toString() {
            return "Lowestnumber is: " + lowestNumber + " Highestnumber is: " + highestNumber;
        }
    }

    /**
     * The Record class containing information about each Record
     */
    private static class Record {
        String id;
        int year;
        BigDecimal houses;
        BigDecimal km2;
        List<String> links = new ArrayList<>();
        NoteState note;
        int yearUsedToCalculate;

        /**
         * Returns the Record as a String
         *
         * @return String
         */
        public String toString() {
            return "ID: " + id + " - Year: " + year + " - Houses: " + houses + " - KM2: " + km2 + " - Links: " + links.toString() + " - Note: " + note + " - Year used to calculate: " + yearUsedToCalculate;
        }
    }

    /**
     * The SquareKilometreRecord class containing square kilometres per Link Code
     */
    private static class SquareKilometreRecord implements Comparable<SquareKilometreRecord> {
        String linkCode;
        Map<Integer, BigDecimal> km2 = new HashMap<>();

        /**
         * Compares the given SquareKilometreRecord with the current SquareKilometreRecord
         *
         * @param sq SquareKilometreRecord containing information about the square kilometres for a specific Link Code
         * @return Integer
         */
        public int compareTo(SquareKilometreRecord sq) {
            return linkCode.compareTo(sq.linkCode);
        }

        /**
         * Returns the value of the SquareKilometreRecord as a String
         *
         * @return String
         */
        public String toString() {
            return linkCode + " - " + km2.keySet() + " - " + km2.values();
        }
    }

    /**
     * The VillageComplex Class used to output the data to the CSV
     */
    private static class VillageComplex {
        Tuple linkCode;
        Map<String, Tuple> yearMap = new HashMap<>();

        /**
         * Returns the toString value of VillageComplex
         *
         * @return String
         */
        public String toString() {
            return "Linkcode value: " + linkCode.value +
                    " Linkcode key: " + linkCode.key +
                    " Linkcode otherLinkCodes: " + linkCode.otherLinkCodes +
                    " Linkcode noteState: " + linkCode.noteState +
                    " yearMap: " + yearMap;
        }
    }

    /**
     * Get the current line number.
     *
     * @return int - Current line number.
     */
    private static int getLineNumber() {
        return Thread.currentThread().getStackTrace()[2].getLineNumber();
    }
}
