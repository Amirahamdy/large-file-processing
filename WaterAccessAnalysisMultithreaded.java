import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class WaterAccessAnalysisMultithreaded {

    public static void main(String[] args) throws Exception {
        long startTime = System.nanoTime();

        String filePath = "C:\\Users\\elwady\\Desktop\\dataset.csv"; 
        List<Map<String, String>> data;

        try {
            System.out.println("Reading data from: " + filePath);
            data = readCSV(filePath);
            System.out.println("Successfully read " + data.size() + " records");
        } catch (IOException e) {
            System.err.println("Error reading file:");
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(4);

        Callable<Set<String>> uniqueCountriesTask = () -> {
            Set<String> uniqueCountries = new HashSet<>();
            for (Map<String, String> row : data) {
                String country = row.get("GeoAreaName");
                if (country != null && !country.isEmpty()) {
                    uniqueCountries.add(country);
                }
            }
            System.out.println("[Thread-UniqueCountries] Unique countries: " + uniqueCountries.size());
            return uniqueCountries;
        };

        Callable<String[]> highestLowestAccessTask = () -> {
            double highest = -1, lowest = Double.MAX_VALUE;
            String highName = "", lowName = "", highYear = "2021", lowYear = "2021";
            for (Map<String, String> row : data) {
                String country = row.get("GeoAreaName");
                try {
                    String value = row.getOrDefault("2021", "").replace("%", "").trim();
                    if (!value.isEmpty()) {
                        double access = Double.parseDouble(value);
                        if (access > highest) {
                            highest = access;
                            highName = country;
                        }
                        if (access < lowest) {
                            lowest = access;
                            lowName = country;
                        }
                    }
                } catch (NumberFormatException ignored) {}
            }
            System.out.printf("[Thread-HighestLowest] Highest: %s (%s) %.2f%%\n", highName, highYear, highest);
            System.out.printf("[Thread-HighestLowest] Lowest: %s (%s) %.2f%%\n", lowName, lowYear, lowest);
            return new String[]{highName, highYear, String.valueOf(highest), lowName, lowYear, String.valueOf(lowest)};
        };

        Callable<double[]> ruralAverageTask = () -> {
            double total = 0; int count = 0;
            String[] years = getYears();
            for (Map<String, String> row : data) {
                if ("RURAL".equalsIgnoreCase(row.get("Location"))) {
                    for (String year : years) {
                        try {
                            String value = row.getOrDefault(year, "").replace("%", "").trim();
                            if (!value.isEmpty()) {
                                double access = Double.parseDouble(value);
                                total += access;
                                count++;
                            }
                        } catch (NumberFormatException ignored) {}
                    }
                }
            }
            double avg = count > 0 ? total / count : 0;
            System.out.printf("[Thread-Rural] Average rural: %.2f%% \n", avg, count);
            return new double[]{avg, count};
        };

        Callable<double[]> urbanAverageTask = () -> {
            double total = 0; int count = 0;
            String[] years = getYears();
            for (Map<String, String> row : data) {
                if ("URBAN".equalsIgnoreCase(row.get("Location"))) {
                    for (String year : years) {
                        try {
                            String value = row.getOrDefault(year, "").replace("%", "").trim();
                            if (!value.isEmpty()) {
                                double access = Double.parseDouble(value);
                                total += access;
                                count++;
                            }
                        } catch (NumberFormatException ignored) {}
                    }
                }
            }
            double avg = count > 0 ? total / count : 0;
            System.out.printf("[Thread-Urban] Average urban: %.2f%% \n", avg, count);
            return new double[]{avg, count};
        };

        Future<Set<String>> uniqueFuture = executor.submit(uniqueCountriesTask);
        Future<String[]> highLowFuture = executor.submit(highestLowestAccessTask);
        Future<double[]> ruralFuture = executor.submit(ruralAverageTask);
        Future<double[]> urbanFuture = executor.submit(urbanAverageTask);

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("\n===== Parallel Analysis Results =====");
        System.out.println("Unique countries: " + uniqueFuture.get().size());

        String[] highLow = highLowFuture.get();
        System.out.printf("Highest water access in 2021: %s with %.2f%%\n", highLow[0], Double.parseDouble(highLow[2]));
        System.out.printf("Lowest water access in 2021: %s with %.2f%%\n", highLow[3], Double.parseDouble(highLow[5]));

        double[] rural = ruralFuture.get();
        double[] urban = urbanFuture.get();
        System.out.printf("Average rural water access: %.2f%% \n", rural[0], rural[1]);
        System.out.printf("Average urban water access: %.2f%% \n", urban[0], urban[1]);

        long endTime = System.nanoTime();
        double durationInSec = (endTime - startTime) / 1e9;
        System.out.printf("[Multithreaded] Total time: %.3f s\n", durationInSec);
    }

    private static String[] getYears() {
        String[] years = new String[23];
        for (int i = 0; i < 23; i++) {
            years[i] = String.valueOf(2000 + i);
        }
        return years;
    }

    public static List<Map<String, String>> readCSV(String filePath) throws IOException {
        List<Map<String, String>> data = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "ISO-8859-1"))) {
            String headerLine = reader.readLine();
            if (headerLine == null) return data;

            List<String> headers = parseCSVLine(headerLine);
            String line;
            while ((line = reader.readLine()) != null) {
                List<String> values = parseCSVLine(line);
                if (values.size() != headers.size()) continue;
                Map<String, String> record = new HashMap<>();
                for (int i = 0; i < headers.size(); i++) {
                    record.put(headers.get(i), values.get(i));
                }
                data.add(record);
            }
        }
        return data;
    }


    private static List<String> parseCSVLine(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean insideQuotes = false;
        for (char c : line.toCharArray()) {
            if (c == '\"') {
                insideQuotes = !insideQuotes;
            } else if (c == ',' && !insideQuotes) {
                result.add(sb.toString().trim().replace("\"", ""));
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        result.add(sb.toString().trim().replace("\"", ""));
        return result;
    }
}
