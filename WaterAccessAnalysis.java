import java.io.*;
import java.util.*;

public class WaterAccessAnalysis {

    public static void main(String[] args) {
        String filePath = "C:\\Users\\elwady\\Desktop\\dataset.csv";
        try {
            System.out.println("Reading data from: " + filePath);
            List<Map<String, String>> data = readCSV(filePath);
            
            if (data.isEmpty()) {
                System.out.println("Warning: No data records were found in the file");
                return;
            }
            
            System.out.println("Successfully read " + data.size() + " records");
            analyzeData(data);
        } catch (IOException e) {
            System.err.println("Error reading file:");
            e.printStackTrace();
        }
    }

    public static List<Map<String, String>> readCSV(String filePath) throws IOException {
        List<Map<String, String>> data = new ArrayList<>();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String headerLine = reader.readLine();
            if (headerLine == null) return data;
            
            String[] headers = headerLine.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            for (int i = 0; i < headers.length; i++) {
                headers[i] = headers[i].trim().replace("\"", "");
            }

            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                
                String[] values = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                if (values.length != headers.length) continue;

                Map<String, String> record = new LinkedHashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    String value = values[i].trim().replace("\"", "");
                    record.put(headers[i], value.isEmpty() ? "0" : value);
                }
                data.add(record);
            }
        }
        return data;
    }

    public static void analyzeData(List<Map<String, String>> data) {
        if (data == null || data.isEmpty()) {
            System.out.println("No data available for analysis");
            return;
        }

        // Use a Set to track unique countries
        Set<String> uniqueCountries = new HashSet<>();
        double highestWaterAccess = -1;
        double lowestWaterAccess = Double.MAX_VALUE;
        String highestCountry = "";
        String lowestCountry = "";
        double ruralAccess = 0, urbanAccess = 0;
        int ruralCount = 0, urbanCount = 0;
        String targetYear = "2021"; // السنة المطلوبة (2021)

        for (Map<String, String> record : data) {
            String geoAreaName = record.get("GeoAreaName");
            String location = record.get("Location");

            // Add to unique countries (only once per area)
            if (geoAreaName != null && !geoAreaName.isEmpty() && !geoAreaName.equals("0")) {
                uniqueCountries.add(geoAreaName);
            }

            // Analyze only the target year (2021)
            String waterAccessStr = record.get(targetYear);
            if (waterAccessStr != null && !waterAccessStr.isEmpty() && !waterAccessStr.equals("0")) {
                try {
                    double waterAccess = Double.parseDouble(waterAccessStr.replace("%", "").trim());
                    
                    if (waterAccess > highestWaterAccess) {
                        highestWaterAccess = waterAccess;
                        highestCountry = geoAreaName;
                    }
                    if (waterAccess < lowestWaterAccess) {
                        lowestWaterAccess = waterAccess;
                        lowestCountry = geoAreaName;
                    }
                    
                    if (location != null) {
                        if (location.equalsIgnoreCase("Rural")) {
                            ruralAccess += waterAccess;
                            ruralCount++;
                        } else if (location.equalsIgnoreCase("Urban")) {
                            urbanAccess += waterAccess;
                            urbanCount++;
                        }
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Invalid number format for " + geoAreaName + 
                                     " in year " + targetYear + ": " + waterAccessStr);
                }
            }
        }

        // Output results
        System.out.println("\n===== Analysis Results for Year " + targetYear + " =====");
        System.out.println("Number of unique countries/areas: " + uniqueCountries.size());
        System.out.printf("Highest water access: %s with %.2f%%\n", 
                         highestCountry, highestWaterAccess);
        System.out.printf("Lowest water access: %s with %.2f%%\n", 
                        lowestCountry, lowestWaterAccess);

        if (ruralCount > 0) {
            System.out.printf("Average rural water access: %.2f%% (%d records)\n", 
                            (ruralAccess / ruralCount), ruralCount);
        } else {
            System.out.println("No rural data available for " + targetYear);
        }

        if (urbanCount > 0) {
            System.out.printf("Average urban water access: %.2f%% (%d records)\n", 
                            (urbanAccess / urbanCount), urbanCount);
        } else {
            System.out.println("No urban data available for " + targetYear);
        }
    }
}