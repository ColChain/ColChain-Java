package org.relweb;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.apache.jena.tdb.transaction.Transaction;
import org.colchain.colchain.knowledgechain.impl.ChainEntry;
import org.colchain.colchain.transaction.ITransaction;
import org.colchain.colchain.transaction.Operation;
import org.colchain.colchain.transaction.impl.TransactionFactory;
import org.colchain.colchain.util.CryptoUtils;
import org.colchain.colchain.util.RandomString;
import org.colchain.index.util.Triple;
import org.colchain.index.util.Tuple;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.listener.ProgressOut;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import java.io.*;
import java.security.KeyPair;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class FragmentMaker {
    public static void main(String[] args) throws IOException, ParserException, NotFoundException, ParseException {
        HDT hdt = HDTManager.loadIndexedHDT("hdt/mappingbased_objects_en.hdt");

        IteratorTripleString it = hdt.search("", "http://dbpedia.org/ontology/incumbent", "");

        HDT hdt1 = HDTManager.generateHDT(it, "http://dbpedia.org/ontology/incumbent", new HDTSpecification(), null);
        hdt1.saveToHDT("new.hdt", null);
    }

    private static int random(int min, int max) {
        return min + (int)(Math.random() * ((max - min) + 1));
    }
}
