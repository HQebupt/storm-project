package main;

import java.util.ArrayList;
import java.util.Collections;

public class Test {

    public static void main(String[] args) {
        String s = "mmllp";
        s = s.substring(0, 2);
        boolean mm = s.equalsIgnoreCase("MM");
        System.out.println(s + " " + mm);
    }

    ArrayList<Integer> sort() {
        ArrayList<Integer> arr = new ArrayList<Integer>();
        for (int i = 10; i > 0; i--) {
            arr.add(i);
        }
        Collections.sort(arr);
        return arr;
    }
}
