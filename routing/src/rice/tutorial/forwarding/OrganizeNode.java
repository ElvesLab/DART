package rice.tutorial.forwarding;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class OrganizeNode {
    public static void main(String[] args) {
        HashMap<String, Integer> level_1 = new HashMap<>();
        HashMap<String, Integer> level_2 = new HashMap<>();
        HashMap<String, Integer> level_3 = new HashMap<>();
        HashMap<String, Integer> level_4 = new HashMap<>();
        Map<String, Integer> level_1_neighbor = new HashMap<>();
        Map<String, Integer> level_2_neighbor = new HashMap<>();
        Map<String, Integer> level_3_neighbor = new HashMap<>();
        Map<String, Integer> level_4_neighbor = new HashMap<>();
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader("MyFile.txt"));
            String line = reader.readLine();
            while (line != null) {
                String[] stringList = line.split(",");
                for (String s : stringList) {
                    if (s.contains("The 1 level")) {
                        level_1.put(s.split("level")[1], level_1.getOrDefault(s.split("level")[1], 0) + 1);
                    }
                    if (s.contains("The 2 level")) {
                        level_2.put(s.split("level")[1], level_2.getOrDefault(s.split("level")[1], 0) + 1);
                    }
                    if (s.contains("The 3 level")) {
                        level_3.put(s.split("level")[1], level_3.getOrDefault(s.split("level")[1], 0) + 1);
                    }
                    if (s.contains("The 4 level")) {
                        level_4.put(s.split("level")[1], level_4.getOrDefault(s.split("level")[1], 0) + 1);
                    }
                    if (s.contains("1 level Neighbor")) {
                        level_1_neighbor.put(s.split("Neighbor")[1], level_1_neighbor.getOrDefault(s.split("Neighbor")[1], 0) + 1);
                    }
                    if (s.contains("2 level Neighbor")) {
                        level_2_neighbor.put(s.split("Neighbor")[1], level_2_neighbor.getOrDefault(s.split("Neighbor")[1], 0) + 1);
                    }
                    if (s.contains("3 level Neighbor")) {
                        level_3_neighbor.put(s.split("Neighbor")[1], level_3_neighbor.getOrDefault(s.split("Neighbor")[1], 0) + 1);
                    }
                    if (s.contains("4 level Neighbor")) {
                        level_4_neighbor.put(s.split("Neighbor")[1], level_4_neighbor.getOrDefault(s.split("Neighbor")[1], 0) + 1);
                    }
                }

//                System.out.println(line);
                // read next line
                line = reader.readLine();
            }
            Map<String, Integer> level_1sorted = sortByValues(level_1);
            Map<String, Integer> level_2sorted = sortByValues(level_2);
            Map<String, Integer> level_3sorted = sortByValues(level_3);
            Map<String, Integer> level_4sorted = sortByValues(level_4);
            System.out.println("---------------Level 1-------------");
            level_1sorted.entrySet().forEach(entry->{
                System.out.println(entry.getKey() + " " + entry.getValue());
            });
//            System.out.println("---------------Level 1 Neighbor-------------");
//            level_1_neighbor.entrySet().forEach(entry->{
//                System.out.println(entry.getKey() + " " + entry.getValue());
//            });
            System.out.println("---------------Level 2-------------");
            level_2sorted.entrySet().forEach(entry->{
                System.out.println(entry.getKey() + " " + entry.getValue());
            });
//            System.out.println("---------------Level 2 Neighbor-------------");
//            level_2_neighbor.entrySet().forEach(entry->{
//                System.out.println(entry.getKey() + " " + entry.getValue());
//            });
            System.out.println("---------------Level 3-------------");
            level_3sorted.entrySet().forEach(entry->{
                System.out.println(entry.getKey() + " " + entry.getValue());
            });
//            System.out.println("---------------Level 3 Neighbor-------------");
//            level_3_neighbor.entrySet().forEach(entry->{
//                System.out.println(entry.getKey() + " " + entry.getValue());
//            });
            System.out.println("---------------Level 4-------------");
            level_4sorted.entrySet().forEach(entry->{
                System.out.println(entry.getKey() + " " + entry.getValue());
            });
//            System.out.println("---------------Level 4 Neighbor-------------");
//            level_4_neighbor.entrySet().forEach(entry->{
//                System.out.println(entry.getKey() + " " + entry.getValue());
//            });
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HashMap sortByValues(HashMap map) {
        List list = new LinkedList(map.entrySet());
        // Defined Custom Comparator here
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                return ((Comparable) ((Map.Entry) (o2)).getValue())
                        .compareTo(((Map.Entry) (o1)).getValue());
            }
        });

        // Here I am copying the sorted list in HashMap
        // using LinkedHashMap to preserve the insertion order
        HashMap sortedHashMap = new LinkedHashMap();
        for (Iterator it = list.iterator(); it.hasNext();) {
            Map.Entry entry = (Map.Entry) it.next();
            sortedHashMap.put(entry.getKey(), entry.getValue());
        }
        return sortedHashMap;
    }
}
