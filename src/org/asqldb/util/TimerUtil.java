/*
 * Copyright (c) 2014, Dimitar Misev
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * 
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.asqldb.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Global timers, identified by string names.
 *
 * @author Dimitar Misev
 */
public class TimerUtil {

    private final static Map<String, Timer> timers = new HashMap<String, Timer>();

    public static Timer getTimer(String name) {
        Timer ret = timers.get(name);
        return ret;
    }

    public static void startTimer(String name) {
        Timer timer = getTimer(name);
        if (timer == null) {
            timer = new Timer(name);
            timers.put(name, timer);
        }
        timer.start();
    }

    public static String stopTimer(String name) {
        Timer timer = getTimer(name);
        if (timer != null) {
            timer.stop();
            return timer.toString();
        }
        return "";
    }

    public static void resetTimer(String name) {
        Timer timer = getTimer(name);
        if (timer != null) {
            timer.reset();
        }
    }

    public static void removeTimer(String name) {
        timers.remove(name);
    }

    public static String getElapsedMilli(String name) {
        Timer timer = getTimer(name);
        if (timer != null) {
            return timer.toString();
        }
        return "";
    }

    public static void printAllTimers() {
        System.out.println("Timers:");
        List<Timer> allTimers = new ArrayList<Timer>();
        allTimers.addAll(timers.values());
        Collections.sort(allTimers);
        for (Timer timer : allTimers) {
            System.out.println(" " + timer.toString());
        }
        System.out.println("");
    }

    public static void clearTimers() {
        timers.clear();
    }

    public final static class Timer implements Comparable<Timer> {

        private final String name;
        private boolean running;
        private long start;
        private long elapsed;

        public Timer(String name) {
            this.name = name;
            reset();
        }

        public void reset() {
            this.elapsed = 0;
            this.running = false;
        }

        public void start() {
            if (!running) {
                start = System.nanoTime();
                running = true;
            }
        }

        public void stop() {
            if (running) {
                elapsed += System.nanoTime() - start;
                running = false;
            }
        }

        public boolean isRunning() {
            return running;
        }

        public long getElapsedNano() {
            long ret = 0;
            if (running) {
                ret = elapsed + (System.nanoTime() - start);
            } else {
                ret = elapsed;
            }
            return ret;
        }

        public long getElapsedMilli() {
            long elapsedNano = getElapsedNano();
            long ret = elapsedNano / 1000000;
            return ret;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[").append(name).append("] ").append(getElapsedMilli()).append(" ms");
            return sb.toString();
        }

        @Override
        public int compareTo(Timer o) {
            return (int) (o.getElapsedNano() - getElapsedNano());
        }

    }
}
