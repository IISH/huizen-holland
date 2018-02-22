package org.iish.dorpen;

import org.apache.commons.collections4.MapIterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.poi.hssf.extractor.ExcelExtractor;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.text.*;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    //    private static final Map<String, Record> records = new HashMap<>(); // Contains the records from the csv
    private static final TreeMap<String, Record> records = new TreeMap<>(); // Contains the records from the csv
    private static final Map<String, Set<String>> codesToIds = new HashMap<>(); // Contains information about link codes belonging to the ids
    private static final Map<String, Set<String>> codeHierarchy = new HashMap<>(); // Contains information about possible children of parents
    private static final List<String> codes = new ArrayList<>(); // Contains all the codes from the csv file
    private static final List<String> handledLinks = new ArrayList<>();
    private static BigDecimal numberOfHouses = new BigDecimal(0);
    private static int record_id_counter = 2;
    private static Set<String> recordsToRemove = new TreeSet<>();
    private static final Map<String, Record> recordsToAdd = new HashMap<>();

    private static final CSVFormat csvFormat = CSVFormat.EXCEL
            .withFirstRecordAsHeader()
            .withDelimiter(';')
            .withIgnoreEmptyLines()
            .withNullString("");

    // TODO: 08-01-2018 With some records where the number of houses is equally divided, the rows are merged, but it should not happen if the ID is the same for the links for each input record.
    // TODO: 08-01-2018 E.G. HO0040A, HO0040B, HO0041
    // TODO: so create a check for when merging if the number of houses need to be added instead of just merging.


    /**
     * De variable “Plaats” is volstrekt irrelevant. Ik denk dat het beter is als je die plaatsnamen gewoon verwijderd uit de originele data.
     * Het gaat alleen om de codes. Ik heb een nieuwe versie bijgevoegd, waarin ik ook nog een foutje uit mijn data heb verbeterd.
     *
     * Ik heb even een handvol codes handmatig gecontroleerd, maar er gaat erg veel mis.
     * Is het misschien een idee om op korte termijn even face to face door te nemen wat er moet gebeuren?
     *
     * Ik zag trouwens dat er allerlei samengestelde cellen in de Excel data zitten. Dat is onhandig voor verdere bewerking, dus dat kan er beter uit.
     * */

    /**
     * The main method to start it all
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String importCsv = args[0];
        String exportCsv = args[1];

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        System.out.println(dateFormat.format(date)); //2016/11/16 12:08:43

        loadData(importCsv);

        updateLinks();

        export(exportCsv);

        date = new Date();

        System.out.println(dateFormat.format(date)); //2016/11/16 12:08:43
    }

    /**
     * Converts the number given to a local BigDecimal
     *
     * @param number String
     * @return BigDecimal
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
     * @param csvPath
     * @throws Exception
     */
    private static void loadData(String csvPath) throws Exception {
        CSVParser parser = CSVParser.parse(new File(csvPath), Charset.forName("UTF-8"), csvFormat);
        parser.forEach(record -> {
            Record r = new Record();
//            r.id = record.get("ID");
            r.id = Integer.toString(record_id_counter);
//            r.locality = record.get("LOCALITY");
            r.year = new Integer(record.get("YEAR"));
//            r.houses = record.get("HOUSES") != null ? convertToLocale(record.get("HOUSES")).setScale(3, BigDecimal.ROUND_HALF_EVEN) : new BigDecimal(0).setScale(3, BigDecimal.ROUND_HALF_EVEN);
//            r.houses = record.get("HOUSES") != null ? new BigDecimal(record.get("HOUSES")).setScale(3, BigDecimal.ROUND_HALF_EVEN) : new BigDecimal(0).setScale(3, BigDecimal.ROUND_HALF_EVEN);
            r.houses = record.get("HOUSES") != null ? new BigDecimal(record.get("HOUSES")) : new BigDecimal(0);
            numberOfHouses = numberOfHouses.add(r.houses);
//            r.persons = record.get("PERSONS") != null ? convertToLocale(record.get("PERSONS")).setScale(3, BigDecimal.ROUND_HALF_EVEN) : null;
//            r.householdSize = record.get("HOUSEHOLDSIZE") != null ? convertToLocale(record.get("HOUSEHOLDSIZE")).setScale(3,BigDecimal.ROUND_HALF_EVEN) : null;
            r.km2 = record.get("KM2") != null ? convertToLocale(record.get("KM2")).setScale(3, BigDecimal.ROUND_HALF_EVEN) : null;
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
                        System.out.println(getLineNumber() + " - Code that not corresponds to default: " + code);
                    }
                }
            } else {
                r.links.add("");
            }
            records.put(r.id, r);
            record_id_counter++;
        });
        System.out.println("Number of houses in total is: " + numberOfHouses);
    }

    /**
     * Sets the parent relation for each of the link codes
     *
     * @param code
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
     * @param code
     */
    private static void updateLinksForCode(String code) {
        if (!codes.contains(code)) {
            codes.add(code);
        }
        if (codeHierarchy.containsKey(code)) {
            for (String parentCode : codeHierarchy.get(code)) {
//                Set<Long> ids = codesToIds.getOrDefault(parentCode, new HashSet<>());
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
     * @param exportPath
     * @throws Exception
     */
    private static void export(String exportPath) throws Exception {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(exportPath))) {
            CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat);

            List<String> headerRow = new ArrayList<>();
            Set<String> headerSet = new HashSet<>();
            headerRow.add("Code");
            // NOTE: a bit time consuming?
            for (Map.Entry<String, Record> entry : records.entrySet()) {
                Record record = entry.getValue();
                headerSet.add(String.valueOf(record.year));
            }

            List<String> tempHeaderRow = new ArrayList<>(headerSet);

            Collections.sort(tempHeaderRow);
            headerRow.addAll(tempHeaderRow);

            csvPrinter.printRecord(headerRow);

            List<String> codesUsed = new ArrayList<>();
            SortedMap<String, VillageComplex> dorpenCollected = new TreeMap<>();

            SortedMap<String, Set<String>> codesToIdsSorted = new TreeMap<>();
            SortedSet<String> keys = new TreeSet<>(codesToIds.keySet());
            for (String key : keys) {
                Set<String> value = codesToIds.get(key);
                codesToIdsSorted.put(key, value);
            }

            int number_of_records_with_multiple_links = 0;
            Set<String> records_to_alter = new TreeSet<>();

            for(Map.Entry<String, Record> record : records.entrySet()){
                if(record.getValue().links.size() > 1) {
                    number_of_records_with_multiple_links++;
                }
                for(Map.Entry<String, Set<String>> code_hierarchy : codeHierarchy.entrySet()){
                    if(record.getValue().links.containsAll(code_hierarchy.getValue())){
                        records_to_alter.add(record.getValue().id);
                    }else if(record.getValue().links.contains(code_hierarchy.getKey())){
                        records_to_alter.add(record.getValue().id);
                    }
                }
            }

            for(Map.Entry<String, Set<String>> code_to_id : codesToIds.entrySet()){
                if(records_to_alter.containsAll(code_to_id.getValue())){
                    for(Record record : records.values()){
                        if(code_to_id.getValue().contains(record.id)){
//                            System.out.println(getLineNumber() + " -> " + code_to_id.getKey() + " - " + record.links);
//                            System.out.println(getLineNumber() + " -> " + codeHierarchy.entrySet().stream().anyMatch(map -> map.getValue().contains(code_to_id.getKey())));
                            if(codeHierarchy.keySet().contains(code_to_id.getKey()) || codeHierarchy.entrySet().stream().anyMatch(map -> map.getValue().contains(code_to_id.getKey()))) {
                                record.links.remove(code_to_id.getKey());
                                String parent = "";
                                for (Map.Entry<String, Set<String>> code_hierarchy_entry : codeHierarchy.entrySet()) {
                                    if (code_hierarchy_entry.getValue().contains(code_to_id.getKey())) {
//                                        System.out.println(getLineNumber() + " -> " + code_hierarchy_entry.getKey());
                                        parent = code_hierarchy_entry.getKey();
                                    }
                                }
//                                System.out.println(getLineNumber() + " -> " + parent + " - " + record.links);
                                if (!parent.equals("") && !record.links.contains(parent)) {
                                    record.links.add(parent);
                                }
                            }
                        }
                    }
                }
            }

//            for (Record record : records.values()) {
//                System.out.println(getLineNumber() + " ->" + record.toString());
//            }
//            System.out.println(getLineNumber() + " -> " + number_of_records_with_multiple_links);

            Collections.sort(codes);


            // NOTE: So far so good!!
            // TODO: Update the codesToIds map after each loop
            if (true) {
                while (number_of_records_with_multiple_links != 0) {
                    for(Map.Entry<String, Set<String>> code_to_id : codesToIds.entrySet()){
                        System.out.println(getLineNumber() + " -> " + code_to_id.getKey() + " - " + code_to_id.getValue());
                    }
                    System.out.println(getLineNumber() + " -> " + codeHierarchy.values());

                    for (Map.Entry<String, Record> record : records.entrySet()) {
                        if (record.getValue().links.size() > 1) {
                            System.out.println(getLineNumber() + " -> " + record.getKey());
                            Map<String, Set<String>> childLinkMap = new HashMap<>();
                            // check if the links complete a parent
                            int difference_between_years = Integer.MAX_VALUE;
                            int closest_difference = Integer.MAX_VALUE;
                            String record_id = Integer.toString(Integer.MAX_VALUE);

//                        List<Map<String, Set<String>>> code_maps_list = new ArrayList<>();
                            Map<String, Set<String>> code_map = new TreeMap<>();
                            for (String link : record.getValue().links) {
                                System.out.println(link);
                                for (Map.Entry<String, Set<String>> codeToIdEntry : codesToIds.entrySet()) {
//                                    System.out.println(getLineNumber() + " -> " + codeToIdEntry.getKey() + " - " + link);
                                    if (codeToIdEntry.getKey().equals(link)) {
//                                    System.out.println(getLineNumber() + " -> " + codeToIdEntry.getKey() + " - " + codeToIdEntry.getValue());
                                        if (!code_map.entrySet().stream().anyMatch(map -> map.getValue().equals(codeToIdEntry))) {
//                                            System.out.println(getLineNumber());
                                            code_map.put(codeToIdEntry.getKey(), codeToIdEntry.getValue());
                                        }
                                    } else {
                                        List<Set<String>> childLinks = codeHierarchy.entrySet().stream().filter(map -> map.getKey().equals(link)).map(Map.Entry::getValue).collect(Collectors.toList());
                                        if (childLinks.size() > 0) {
//                                        System.out.println(getLineNumber() + " -> " + childLinks.get(0));
                                            for (String child : childLinks.get(0)) {
//                                            System.out.println(getLineNumber() + " -> " + child);
                                                if (codeToIdEntry.getKey().equals(child)) {
//                                                System.out.println(getLineNumber() + " -> " + codeToIdEntry.getKey() + " - " + codeToIdEntry.getValue());
                                                    if (!code_map.entrySet().stream().anyMatch(map -> map.getValue().equals(child)) && codeToIdEntry.getValue().contains(record.getKey())) {
                                                        code_map.put(codeToIdEntry.getKey(), codeToIdEntry.getValue());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            // NOTE: Might need it, not sure
//                            Map<String, Set<String>> code_map_to_append_2 = new HashMap<>();
//                            for(Set<String> codes : code_map.values()){
//                                for(String code : codes) {
//                                    for (Map.Entry<String, Set<String>> code_hier : codesToIds.entrySet()) {
//                                        if (code_hier.getValue().contains(code) && !code_hier.equals(codes)){
//                                            code_map_to_append_2.put(code_hier.getKey(), code_hier.getValue());
//                                        }
//                                    }
//                                }
//                            }
//                            code_map.putAll(code_map_to_append_2);

                            Map<String, Set<String>> code_map_to_append = new TreeMap<>();
                            Set<String> codes_to_remove = new TreeSet<>();
                            Map.Entry<String, Set<String>> firstEntry = code_map.entrySet().iterator().next();
                            Iterator<Map.Entry<String, Set<String>>> it = code_map.entrySet().iterator();
                            while (it.hasNext()) {
                                Map.Entry<String, Set<String>> next = it.next();
                                if (!next.equals(firstEntry)) {
                                    if (next.getKey().substring(0, 6).equals(firstEntry.getKey().substring(0, 6)) && next.getValue().equals(firstEntry.getValue())) {
                                        codes_to_remove.add(firstEntry.getKey());
                                        codes_to_remove.add(next.getKey());
                                        code_map_to_append.put(firstEntry.getKey().substring(0, 6), firstEntry.getValue());
                                    }
                                    firstEntry = next;
                                }
                            }



                            for (String code_to_remove : codes_to_remove) {
                                code_map.remove(code_to_remove);
                            }
                            code_map.putAll(code_map_to_append);

                            for (Map.Entry<String, Set<String>> code_to_id : code_map.entrySet()) {
                                System.out.println(getLineNumber() + " -> " + code_to_id.getKey() + " - " + code_to_id.getValue().toString());
                            }

                            // Gets the unique values and the duplicate values
                            Set<String> duplicates = null;
                            Set<String> uniques = null;
                            for (String key : code_map.keySet()) {
                                if (duplicates == null && uniques == null) {
                                    duplicates = new TreeSet<>(code_map.get(key));
                                    uniques = new TreeSet<>(code_map.get(key));
                                } else {
                                    duplicates.retainAll(code_map.get(key));
                                    uniques.addAll(code_map.get(key));
                                }
                            }
                            uniques.removeAll(duplicates);

                            System.out.println(getLineNumber() + " -> " + duplicates);
                            System.out.println(getLineNumber() + " -> " + uniques);

                            /** Looping through the unique codes to determine duplicate years in the Link codes */
                            TreeSet<Integer> years = new TreeSet<>();

                            Map<String, BigDecimal> uniqueValuesToCalculateFrom = new HashMap<>();
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

                            System.out.println(getLineNumber() + " -> " + years + " - " + uniqueValuesToCalculateFrom.values());

                            /** Determine the value to calculate. These are the codes that are the same, ergo: the codes that need to be split up.*/
                            TreeSet<Integer> duplicateYears = new TreeSet<>();
                            Map<String, BigDecimal> equalValueToCalculate = new HashMap<>();
                            for (String duplicate : duplicates) {
                                for (Map.Entry<String, Record> duplicate_record : records.entrySet()) {
                                    if (duplicate_record.getValue().id.equals(duplicate)) {
//                                    System.out.println(getLineNumber() + " -> " + duplicate);
                                        equalValueToCalculate.put(duplicate, duplicate_record.getValue().houses);
                                        duplicateYears.add(duplicate_record.getValue().year);
                                    }
                                }
                            }

                            System.out.println(getLineNumber() + " -> " + equalValueToCalculate.keySet() + " - " + equalValueToCalculate.values() + " - " + duplicateYears);

                            Integer yearToCheck = Integer.MAX_VALUE;
                            Integer year_diff = Integer.MAX_VALUE;
                            Integer yearToCalculateWith = Integer.MAX_VALUE;
                            Integer year_temp_something_dunno = 0;

                            /** Loop through the map containing the values to calculate */
                            /** Looping through the records to check which record corresponds with the valueToCalculate ID */
                            for (Map.Entry<String, Record> calculate_record : records.entrySet()) {
                                /** Get the year to perform the check upon */
                                if(equalValueToCalculate.containsKey(calculate_record.getValue().id)){
                                    yearToCheck = calculate_record.getValue().year;
                                    System.out.println(getLineNumber() + " -> " + calculate_record.getValue().year);

                                    /** Determine which year is closest to the year to calculate the values for */
                                    for (Integer yearToCalculateFrom : years) {
                                        Integer timeBetweenYears = yearToCalculateFrom - yearToCheck;
                                        if (timeBetweenYears < 0) {
                                            timeBetweenYears = timeBetweenYears * -1;
                                        }
                                        if (timeBetweenYears < year_diff) {
                                            System.out.println(getLineNumber() + " -> " + timeBetweenYears + " - " + year_diff + " - " + yearToCalculateFrom);
                                            year_temp_something_dunno = yearToCalculateFrom;
                                            year_diff = timeBetweenYears;
                                            yearToCalculateWith = calculate_record.getValue().year;
                                        }
                                    }
                                }

                            }

                            System.out.println(getLineNumber() + " -> " + year_diff + " - " + yearToCalculateWith);

                            Map<BigDecimal, String> valuesToCalculateWithMap = new HashMap<>();
                            /** Loop through the Map of unique values to perform the calculation */
                            for (Map.Entry<String, BigDecimal> uniqueValue : uniqueValuesToCalculateFrom.entrySet()) {
                                /** Loop through the records to get the record for which the id and year are the same as the given unique value */
                                for (Map.Entry<String, Record> unique_value_record : records.entrySet()) {
//                                    System.out.println(getLineNumber() + " -> " + unique_value_record.getValue().id + " - " + uniqueValue.getKey()
//                                            + " - " + unique_value_record.getValue().year + " - " + year_temp_something_dunno);
                                    if (unique_value_record.getValue().id.equals(uniqueValue.getKey()) && unique_value_record.getValue().year == year_temp_something_dunno) {
                                        valuesToCalculateWithMap.put(uniqueValue.getValue(), unique_value_record.getValue().links.toString());
                                    }
                                }
                            }

                            System.out.println(getLineNumber() + " -> " + valuesToCalculateWithMap.values());

                            /** Check if the Map with unique values is bigger than 0 */
                            if (valuesToCalculateWithMap.size() > 0) {

                                List<BigDecimal> values = new ArrayList<>();
                                /** Sort the values so the first value is the smallest one for calculation purposes */
                                for (Map.Entry<BigDecimal, String> value_to_calculate : valuesToCalculateWithMap.entrySet()) {
                                    values.add(value_to_calculate.getKey());
                                }
                                Collections.sort(values);
                                System.out.println(getLineNumber() + " -> " + values);

                                /** Loop through the values to calculate in order to get the correct number of houses based on the proportions */
                                for (Map.Entry<String, BigDecimal> valueToCalculate : equalValueToCalculate.entrySet()) {
                                    Pair result;
                                    try {
                                        result = getNumberOfHousesInAccordanceToUpcomingYear(new Pair(values.get(0), values.get(1)), valueToCalculate.getValue());
                                    } catch (Exception ex) {
                                        result = new Pair(new BigDecimal(0), new BigDecimal(0));
                                    }

                                    System.out.println(getLineNumber() + " -> " + result.lowestNumber + " - " + result.highestNumber + " from " + valueToCalculate.getValue() + " - " + valueToCalculate.getKey());
                                    if(!result.lowestNumber.equals(new BigDecimal(0))) {

                                        /** Determine the highest number of the returned value after the calculation */
                                        String linkLowestNumber = valuesToCalculateWithMap.get(values.get(0));
                                        String linkHighestNumber = "";
                                        if (valuesToCalculateWithMap.size() > 1) {
                                            linkHighestNumber = valuesToCalculateWithMap.get(values.get(1));
                                        }
                                        System.out.println(getLineNumber() + " -> " + valuesToCalculateWithMap + " - " + result.lowestNumber + " - " + result.highestNumber);
                                        System.out.println(getLineNumber() + " -> " + valueToCalculate.getKey() + " - " + record.getValue().links.toString());

                                        if (valueToCalculate.getKey().equals(record.getKey())) {
                                            for (int i = 0; i < record.getValue().links.size(); i++) {
                                                Record r = new Record();
                                                r.year = record.getValue().year;
                                                r.links.add(record.getValue().links.get(i));
                                                linkLowestNumber = linkLowestNumber.replace('[', ' ');
                                                linkLowestNumber = linkLowestNumber.replace(']', ' ');
                                                linkLowestNumber = linkLowestNumber.trim();
//                                                System.out.println(getLineNumber() + " -> " + r.links.get(0) + " - " + linkLowestNumber);

                                                r.houses = linkLowestNumber.equals(r.links.get(0)) ? result.lowestNumber : result.highestNumber;
                                                r.id = record.getKey() + record.getValue().year + record.getValue().links.get(i) + r.houses;

//                                                System.out.println(getLineNumber() + " -> " + r.id + " - " + r.year + " - " + r.links + " - " + r.houses);
                                                if (!recordsToAdd.containsValue(r)) {
                                                    recordsToAdd.put(r.id, r);
                                                }
                                            }
//                                            System.out.println(getLineNumber() + " -> " + record.toString());
                                            recordsToRemove.add(valueToCalculate.getKey());
                                        }
                                    }

//                                for (Map.Entry<String, Record> record_to_add : recordsToAdd.entrySet()) {
//                                    System.out.println(getLineNumber() + " -> " + record_to_add.getKey() + " - " + record_to_add.getValue().links + " - " + record_to_add.getValue().houses + " - " + record_to_add.getValue().year);
//                                }
                                }
                                System.out.println(getLineNumber() + " -> " + recordsToAdd.values());
                            }else if(valuesToCalculateWithMap.size() == 0){
//                                // NOTE: The following is put on hold for now!
//
//                                // TODO: create code to calculate the number of homes by using the km2 if there are no items in valuestocalculatewithmap
//                                System.out.println(getLineNumber() + " -> " + years + " - " + uniqueValuesToCalculateFrom.values());
//                                System.out.println(getLineNumber() + " -> " + record.getKey() + " - " + record.getValue().year);
//
//
//                                System.out.println(getLineNumber() + " -> " + record.getValue().links);
//
//                                int year_to_calculate_from = 0;
//                                for(String id : uniques){
//                                    for(Record record_unique : records.values()){
//                                        if(record_unique.id.equals(id) && record_unique.links.size() == 1 && record.getValue().links.contains(record_unique.links.get(0))){
//                                            year_to_calculate_from = record_unique.year;
//                                        }
//                                    }
//                                }
//
//                                System.out.println(getLineNumber() + " -> " + year_to_calculate_from);
//
//                                Set<String> ids_to_calculate_from = new TreeSet<>();
//                                for(String unique : uniques){
//                                    for(Record record_unique : records.values()){
//                                        if(record_unique.id.equals(unique) && record_unique.year == year_to_calculate_from){
//                                            ids_to_calculate_from.add(unique);
//                                        }
//                                    }
//                                }
//
//                                System.out.println(getLineNumber() + " -> " + ids_to_calculate_from);
//
//                                // TODO: use year to calculate from to calculate the values for the next closest year.

                            }
                        }
                    }

                    for (String id : recordsToRemove) {
                        records.remove(id);
                    }
                    System.out.println(getLineNumber() + " -> " + recordsToRemove.toString());
                    recordsToRemove.clear();

                    for (Map.Entry<String, Record> record_to_add : recordsToAdd.entrySet()) {
                        record_to_add.getValue().id = Integer.toString(record_id_counter);
                        records.put(Integer.toString(record_id_counter), record_to_add.getValue());
                        record_id_counter++;
                    }
                    recordsToAdd.clear();
//                    System.out.println(getLineNumber() + " -> " + recordsToAdd.keySet().toString());

                    BigDecimal number_of_homes = new BigDecimal(0).setScale(3, BigDecimal.ROUND_HALF_EVEN);
                    List<Record> sortedRecords = records.values().stream().sorted((t1, t2) -> {
                        if (t1.year == t2.year && t1.links.size() == 1 && t2.links.size() == 1)
                            return t1.links.get(0).compareTo(t2.links.get(0));
                        return t1.year <= t2.year ? -1 : 1;
                    }).collect(Collectors.toList());

                    for (Record record : sortedRecords) {
                        System.out.println(getLineNumber() + " -> " + record.toString());
                        number_of_homes = number_of_homes.add(record.houses);
                    }

                    System.out.println(getLineNumber() + " -> " + number_of_homes);

                    int duplicate_link_code_validator = 0;
                    for (Record record : records.values()) {
                        if (record.links.size() > 1) {
                            duplicate_link_code_validator++;
                        }
                    }

                    System.out.println(getLineNumber() + " -> " + duplicate_link_code_validator + " vs " + number_of_records_with_multiple_links);

                    if (duplicate_link_code_validator == number_of_records_with_multiple_links) {
                        break;
                    } else {
                        number_of_records_with_multiple_links = duplicate_link_code_validator;
                        codeHierarchy.clear();
                        codesToIds.clear();
                        for(Record record : records.values()){
                            for(String code : record.links) {
                                Set<String> ids = codesToIds.getOrDefault(code, new HashSet<>());
//                                codesToIds.clear();
//                                Set<String> ids = codesToIds.getOrDefault(code, new HashSet<>());
                                ids.add(record.id);
                                codesToIds.put(code, ids);

                                setParentRelation(code);
                            }
                        }
                        updateLinks();
                    }
                }
            }


            // Setup to save the info to csv
//            System.out.println(getLineNumber() + " -> " + dorpenCollected.values());
//            for(Map.Entry<String, Set<String>> hierarchy : codeHierarchy.entrySet()) {
//                for (Record record : records.values()) {
//
//                }
//            }


            // NOTE: Ignore below for now, old way of getting data and to save it to the csv file
            // NOTE: Although it seems to be working? Gotta check it.
            /** Looping through the link codes combined with the ids belonging to the code */
            for (String entry : codes) {
                VillageComplex village = new VillageComplex();

                /** Adding the Link codes to a list for further processing, removing duplicates
                 *  And adding the Link to the dorpenComplex map for processing */
                if (!codesUsed.contains(entry)) {
                    codesUsed.add(entry);
                    SortedSet<String> other_link_codes = new TreeSet<>();
                    other_link_codes.add(entry);
                    village.linkCode = new Tuple(entry, "0", true, other_link_codes);
                }

                /** Checking to see if the dorpencomplex Hashmap is empty*/
                if (village.linkCode != null) {
                    /** Looping through the records collected from the CSV file */
                    for (Map.Entry<String, Record> recordEntry : records.entrySet()) {
                        /** Check if the record entry attribute 'links' contains the Link code from dorpencomplex Hashmap
                         *  If so, it adds the place of the record entry in the dorpencomplex Hashmap */
                        for (String link : recordEntry.getValue().links) {
                            if (link.equals(village.linkCode.key)) {
                                /** Tries to put the year belonging to the record in the Hashmap dorpenComplex */
                                try {
                                    if (recordEntry.getValue().links.size() == 1) {
                                        village.linkCode.otherLinkCodes.addAll(recordEntry.getValue().links);
                                        SortedSet<String> set = new TreeSet<>(recordEntry.getValue().links);
                                        village.yearMap.put(Integer.toString(recordEntry.getValue().year), new Tuple(recordEntry.getValue().houses.toString(), recordEntry.getValue().id, true, set));
                                    } else {
                                        village.linkCode.otherLinkCodes.addAll(recordEntry.getValue().links);
                                        SortedSet<String> set = new TreeSet<>(recordEntry.getValue().links);
                                        village.yearMap.put(Integer.toString(recordEntry.getValue().year), new Tuple(recordEntry.getValue().houses.toString(), recordEntry.getValue().id, true, set));
                                    }
                                    break;
                                } catch (NullPointerException e) {
                                    System.out.println("NULLPOINTER!!");
                                }
                            }
                        }
                    }

                    /** Checks if the length of the Link Code is longer than 6 characters
                     *  If so, it gets the code from the codeHierarchy, by checking if the codehierarchy code equals the beginning of the dorpencomplex code */
                    if (village.linkCode.key.length() > 6) {
                        for (Map.Entry<String, Set<String>> codeHierarchyEntry : codeHierarchy.entrySet()) {
                            if (codeHierarchyEntry.getValue().contains(village.linkCode.key)) {
                                dorpenCollected.put(village.linkCode.key, village);
                            }
                        }
                    } else {
                        dorpenCollected.put(village.linkCode.key, village);
                    }
                }
            }

            /** Converts the dorpenCollected so it can be saved in the CSV file! */
            for (Map.Entry<String, VillageComplex> dorpCollected : dorpenCollected.entrySet()) {
                List<String> dorpenOutput = new ArrayList<>();
                for (String s : headerRow) {
                    try {
                        switch (s) {
                            case "Code":
                                dorpenOutput.add(dorpCollected.getValue().linkCode.key);
                                break;
                            default:
                                if (dorpCollected.getValue().yearMap.get(s).key.equals("0")) {
                                    String numberOfHouses = handleHousesBeingZero(dorpCollected, s);
                                    dorpenOutput.add(numberOfHouses);
                                    dorpCollected.getValue().yearMap.get(s).key = numberOfHouses;
                                } else {
                                    dorpenOutput.add(dorpCollected.getValue().yearMap.get(s).key);
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
    }

    /**
     * @param dorpCollected
     * @param year
     * @return
     */
    private static String handleHousesBeingZero(Map.Entry<String, VillageComplex> dorpCollected, String year) {
        String result = "NULL";
        String divider = "None";
        BigDecimal proportionKM2 = new BigDecimal(Integer.MAX_VALUE).setScale(3, BigDecimal.ROUND_HALF_EVEN);
        Record testRecord = new Record();
        for (Map.Entry<String, Record> record : records.entrySet()) {
            if (record.getValue().links.contains(dorpCollected.getKey()) && record.getValue().id.equals(dorpCollected.getValue().yearMap.get(year).value)) {
                testRecord = record.getValue();
            }
        }
        for (Map.Entry<String, Record> record : records.entrySet()) {
            if (record.getValue().links.contains(dorpCollected.getKey()) && !record.getValue().id.equals(dorpCollected.getValue().yearMap.get(year).value)) {
                /** Collect the proportion to calculate the empty value of dorpCollected with */
                if (!record.getValue().houses.equals(new BigDecimal(0))) {
                    BigDecimal houses = record.getValue().houses.setScale(3, RoundingMode.HALF_EVEN);
                    if (record.getValue().km2 != null && testRecord.km2 != null) {
                        BigDecimal km2 = record.getValue().km2.setScale(3, RoundingMode.HALF_EVEN);
                        proportionKM2 = houses.divide(km2, RoundingMode.HALF_EVEN);
                        divider = "km2";
                        break;
                    }
                }
            }
        }
        for (Map.Entry<String, Record> record : records.entrySet()) {
            if (!divider.equals("None")) {
                if (Integer.toString(record.getValue().year).equals(year)) {
                    if (record.getValue().id.equals(dorpCollected.getValue().yearMap.get(year).value)) {
                        if (record.getValue().km2 != null) {
                            result = record.getValue().km2.multiply(proportionKM2).toString();
                            break;
                        }
                    }
                }
            }
        }
        return result;
    }

    private static Pair getNumberOfHousesInAccordanceToUpcomingYear(Pair upcomingYear, BigDecimal numberOfHousesToSplit) {
        BigDecimal percentage = upcomingYear.lowestNumber.divide(upcomingYear.highestNumber.add(upcomingYear.lowestNumber), 9,BigDecimal.ROUND_HALF_EVEN); //.setScale(9, BigDecimal.ROUND_HALF_EVEN);
        BigDecimal lowestResult = numberOfHousesToSplit.multiply(percentage).setScale(3, BigDecimal.ROUND_HALF_EVEN);
        BigDecimal highestResult = numberOfHousesToSplit.subtract(lowestResult).setScale(3, BigDecimal.ROUND_HALF_EVEN);
        return new Pair(lowestResult, highestResult);
    }

    private static class Tuple {
        String key;
        String value;
        SortedSet<String> otherLinkCodes;
        boolean singleCode;

        Tuple(String key, String value, boolean singleCode, SortedSet<String> otherLinkCodes) {
            this.key = key;
            this.value = value;
            this.singleCode = singleCode;
            this.otherLinkCodes = otherLinkCodes;
        }
    }

    private static class Pair {
        BigDecimal lowestNumber;
        BigDecimal highestNumber;

        Pair(BigDecimal low, BigDecimal high) {
            lowestNumber = low;
            highestNumber = high;
        }
    }

    private static class Record {
        String id;
        int year;
        BigDecimal houses;
        BigDecimal km2;
        List<String> links = new ArrayList<>();

        public String toString() {
            return id + " - " + year + " - " + houses + " - " + km2 + " - " + links.toString();
        }
    }

    private static class VillageComplex {
        Tuple linkCode;
        Map<String, Tuple> yearMap = new HashMap<>();
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
