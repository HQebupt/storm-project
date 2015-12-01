package util;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;

public class Comp implements Comparator<Map.Entry<String, Integer>>,
    Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
        return (o2.getValue() - o1.getValue());
    }
}
