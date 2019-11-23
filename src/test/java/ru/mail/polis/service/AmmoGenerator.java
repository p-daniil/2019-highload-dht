package ru.mail.polis.service;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

import static java.nio.charset.StandardCharsets.US_ASCII;

public class AmmoGenerator {
    private static final int VALUE_LENGTH = 256;
    private static final String[] MODES = {
            "1 - Лента с PUTами с уникальными ключами",
            "2 - Лента с PUTами с частичной перезаписью ключей (вероятность 10%)",
            "3 - Лента с GETами существующих ключей с равномерным распределением",
            "4 - То же самое, но со смещением распределения GETов к недавно добавленным ключам",
            "5 - Лента со смешанной нагрузкой с 50% PUTы новых ключей и 50% GETы существующих ключей"
    };

    private static File ammoFile(final int mode) throws IOException {
        final Path dir = Paths.get(".", "tank");
        Files.createDirectories(dir);
        return dir.resolve("ammo" + mode + ".txt").toFile();
    }

    private static void writeAmmoFile(final int mode, Data ammoData) throws IOException {
        try (OutputStream fos = new FileOutputStream(ammoFile(mode))) {
            ammoData.write(fos);
        }
    }

    private static byte[] randomValue() {
        final byte[] result = new byte[VALUE_LENGTH];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    private static void putUniqueKeys(final long requestsCount, OutputStream os) throws IOException {
        for (long i = 0; i < requestsCount; i++) {
            put(Long.toString(i), os);
        }
    }

    private static void getKeys(final long requestsCount, OutputStream os) throws IOException {
        for (long i = 0; i < requestsCount; i++) {
            get(Long.toString(ThreadLocalRandom.current().nextLong(requestsCount)), os);
        }
    }

    private static void putOverwrite(final long requestsCount, OutputStream os) throws IOException {
        long key = 0;
        for (long i = 0; i < requestsCount; i++) {
            if (ThreadLocalRandom.current().nextInt(10) == 0) {
                final String existedKey = Long.toString(ThreadLocalRandom.current().nextLong(key));
                put(existedKey, os);
            } else {
                put(Long.toString(key), os);
                key++;
            }
        }
    }

    private static void getLastKeys(final long requestsCount, OutputStream os) throws IOException {
        for (long i = 0; i < requestsCount; i++) {
            final long key = lastKeyPriority(requestsCount, 30, 90);
            get(Long.toString(key), os);
        }
    }

    private static long lastKeyPriority(final long requestsCount,
                                        final int lastPercent,
                                        final int priorityPercent) {
        final int randomPercent = ThreadLocalRandom.current().nextInt(100);
        final long oldValuesBound = requestsCount * (100 - lastPercent) / 100;
        if (randomPercent > priorityPercent) {
            return ThreadLocalRandom.current().nextLong(oldValuesBound);
        } else {
            return ThreadLocalRandom.current().nextLong(oldValuesBound, requestsCount);
        }
    }

    private static void putAndGet(final long requestsCount, OutputStream os) throws IOException {
        long key = 0;
        put(Long.toString(key), os);
        key++;
        for (long i = 1; i < requestsCount; i++) {
            final boolean nextPut = ThreadLocalRandom.current().nextBoolean();
            if (nextPut) {
                put(Long.toString(key), os);
                key++;
            } else {
                get(Long.toString(ThreadLocalRandom.current().nextLong(key)), os);
            }
        }
    }

    private static byte[] getBytes(String s) {
        return s.getBytes(US_ASCII);
    }

    private static void put(final String key, final OutputStream os) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            final byte[] value = randomValue();
            bos.write(getBytes("PUT /v0/entity?id=" + key + " HTTP/1.1\r\n"));
            bos.write(getBytes("Content-Length: " + value.length + "\r\n"));
            bos.write(value);
            bos.write(getBytes("\r\n"));
            os.write(getBytes(Integer.toString(bos.size())));
            os.write(getBytes(" put\n"));
            bos.writeTo(os);
            os.write(getBytes("\r\n"));
        }
    }

    private static void get(final String key, final OutputStream os) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            bos.write(getBytes("GET /v0/entity?id=" + key + " HTTP/1.1\r\n"));
            os.write(getBytes(Integer.toString(bos.size())));
            os.write(getBytes(" get\n"));
            bos.writeTo(os);
            os.write(getBytes("\r\n"));
        }
    }

    public static void main(String[] args) throws IOException {
        Scanner in = new Scanner(System.in);

        for (String mode : MODES) {
            System.out.println(mode);
        }
        int mode;
        do {
            if (in.hasNextInt()) {
                mode = in.nextInt();
            } else {
                in.next();
                mode = 0;
            }
        } while (mode < 1 || mode > MODES.length);

        System.out.println("Requests count:");
        int requestsCountModifiable;
        do {
            if (in.hasNextInt()) {
                requestsCountModifiable = in.nextInt();
            } else {
                in.next();
                requestsCountModifiable = 0;
            }
        } while (requestsCountModifiable < 1);

        final int requestsCount = requestsCountModifiable;
        switch (mode) {
            case 1:
                writeAmmoFile(mode, fos -> putUniqueKeys(requestsCount, fos));
                break;
            case 2:
                writeAmmoFile(mode, fos -> putOverwrite(requestsCount, fos));
                break;
            case 3:
                writeAmmoFile(mode, fos -> getKeys(requestsCount, fos));
                break;
            case 4:
                writeAmmoFile(mode, fos -> getLastKeys(requestsCount, fos));
                break;
            case 5:
                writeAmmoFile(mode, fos -> putAndGet(requestsCount, fos));
                break;
        }
    }

    @FunctionalInterface
    interface Data {
        void write(OutputStream os) throws IOException;
    }
}
