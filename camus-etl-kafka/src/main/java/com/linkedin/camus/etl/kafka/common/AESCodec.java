/*
 * Copyright 2015 (c) Amadeus
 *
 * Author: Christophe-Marie Duquesne
 */

package com.linkedin.camus.etl.kafka.common;

import java.security.DigestException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class AESCodec {

    private byte[] passphrase;

    public AESCodec(byte[] passphrase) {
        this.passphrase = passphrase;
    }

    public byte [] deriveKeyAndIV(byte[] salt) throws
        DigestException, NoSuchAlgorithmException {

        byte[] res = new byte[32];

        final MessageDigest md5 = MessageDigest.getInstance("MD5");

        md5.update(this.passphrase);
        md5.update(salt);
        byte[] hash1 = md5.digest();

        md5.reset();
        md5.update(hash1);
        md5.update(this.passphrase);
        md5.update(salt);
        byte[] hash2 = md5.digest();

        // copy the hashes in the result array
        System.arraycopy(hash1, 0, res, 0, 16);
        System.arraycopy(hash2, 0, res, 16, 16);
        return res;
    }

    public byte[] decrypt(byte[] data)
            throws DigestException, NoSuchAlgorithmException,
                NoSuchPaddingException, InvalidKeyException,
                InvalidAlgorithmParameterException,
                IllegalBlockSizeException,
                BadPaddingException {

        byte[] salt = Arrays.copyOfRange(data, 8, 16);
        byte[] encrypted = Arrays.copyOfRange(data, 16, data.length);

        byte[] keyAndIV = this.deriveKeyAndIV(salt);
        byte[] key = Arrays.copyOfRange(keyAndIV, 0, 16);
        byte[] iv = Arrays.copyOfRange(keyAndIV, 16, 32);

        SecretKeySpec skeySpec = new SecretKeySpec(key, "AES");
        IvParameterSpec ivspec = new IvParameterSpec(iv);

        Cipher cipher;
        cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.DECRYPT_MODE, skeySpec, ivspec);
        return cipher.doFinal(encrypted);
    }

    public byte[] encrypt(byte[] data)
            throws DigestException, NoSuchAlgorithmException,
                NoSuchPaddingException, InvalidKeyException,
                InvalidAlgorithmParameterException,
                IllegalBlockSizeException, BadPaddingException {
        byte [] salt = new byte [8];
        SecureRandom sr = new SecureRandom();
        sr.nextBytes(salt);

        byte [] keyAndIV = deriveKeyAndIV(salt);
        byte [] key = Arrays.copyOfRange(keyAndIV, 0, 16);
        byte [] iv = Arrays.copyOfRange(keyAndIV, 16, 32);
        SecretKeySpec skeySpec = new SecretKeySpec(key, "AES");
        IvParameterSpec ivspec = new IvParameterSpec(iv);

        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE, skeySpec, ivspec);
        byte [] encrypted = cipher.doFinal(data);

        byte [] res = new byte[16 + encrypted.length];
        System.arraycopy("Salted__".getBytes(), 0, res, 0, 8);
        System.arraycopy(salt, 0, res, 8, 8);
        System.arraycopy(encrypted, 0, res, 16, encrypted.length);
        return res;
    }

}
