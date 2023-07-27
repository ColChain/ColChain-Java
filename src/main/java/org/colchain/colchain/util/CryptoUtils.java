package org.colchain.colchain.util;

import org.colchain.colchain.transaction.ITransaction;

import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

/**
 * @author visruthcv
 *
 */
public class CryptoUtils {
    private static final String ALGORITHM = "RSA";

    public static KeyPair generateKeyPair() {
        try {
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance(ALGORITHM);

            SecureRandom random = SecureRandom.getInstance("SHA1PRNG", "SUN");

            // 512 is keysize
            keyGen.initialize(512, random);

            KeyPair generateKeyPair = keyGen.generateKeyPair();
            return generateKeyPair;
        } catch (Exception e) {
            return null;
        }
    }

    public static byte[] createSignature(PrivateKey key, ITransaction t) {
        Signature sig;
        try {
            sig = Signature.getInstance("SHA256withRSA");
            sig.initSign(key);
            sig.update(t.getBytes());
            return sig.sign();
        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
            return new byte[0];
        }
    }

    public static boolean matches(PublicKey key, ITransaction t, byte[] signature) {
        Signature sig;
        try {
            sig = Signature.getInstance("SHA256withRSA");
            sig.initVerify(key);
            sig.update(t.getBytes());
            return sig.verify(signature);
        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
            return false;
        }
    }

    public static PublicKey getPublicKey(byte[] bytes) {
        try {
            return KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(bytes));
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            return null;
        }
    }

    public static PrivateKey getPrivateKey(byte[] bytes) {
        try {
            return KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(bytes));
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            return null;
        }
    }
}
