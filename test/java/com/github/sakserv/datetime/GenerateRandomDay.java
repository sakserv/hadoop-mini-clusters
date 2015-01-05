package com.github.sakserv.datetime;

import java.util.GregorianCalendar;

public class GenerateRandomDay {

    public static String genRandomDay() {

        GregorianCalendar gc = new GregorianCalendar();

        int year = randBetween(2013, 2014);

        gc.set(gc.YEAR, year);

        int dayOfYear = randBetween(1, gc.getActualMaximum(gc.DAY_OF_YEAR));

        gc.set(gc.DAY_OF_YEAR, dayOfYear);

        return String.format("%04d-%02d-%02d", gc.get(gc.YEAR), gc.get(gc.MONTH), gc.get(gc.DAY_OF_MONTH));

    }

    public static int randBetween(int start, int end) {
        return start + (int)Math.round(Math.random() * (end - start));
    }
}