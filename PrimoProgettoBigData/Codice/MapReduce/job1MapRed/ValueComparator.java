package PrimoProgetto.job1MapRed;

import java.util.Comparator;
import java.util.Map;

public class ValueComparator implements Comparator<String> {

    private Map<String,Integer> map;

    public ValueComparator(Map<String,Integer> map) {
        this.map = map;
    }

    public int compare(String a, String b) {
        int result =  map.get(b).compareTo(map.get(a));
        if(result == 0) return 1;
        		return result;
    }
}