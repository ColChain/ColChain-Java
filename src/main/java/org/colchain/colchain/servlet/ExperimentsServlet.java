package org.colchain.colchain.servlet;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.jena.tdb.store.Hash;
import org.colchain.colchain.util.*;
import org.colchain.index.util.Triple;
import org.colchain.index.util.Tuple;
import org.colchain.colchain.community.Community;
import org.colchain.colchain.community.CommunityMember;
import org.colchain.colchain.knowledgechain.impl.ChainEntry;
import org.colchain.colchain.knowledgechain.impl.KnowledgeChain;
import org.colchain.colchain.node.AbstractNode;
import org.colchain.colchain.sparql.ColchainJenaConstants;
import org.colchain.colchain.sparql.graph.ColchainGraph;
import org.colchain.colchain.transaction.ITransaction;
import org.colchain.colchain.transaction.Operation;
import org.colchain.colchain.writer.IResponseWriter;
import org.colchain.colchain.writer.ResponseWriterFactory;
import org.apache.commons.io.FileUtils;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.listener.ProgressOut;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class ExperimentsServlet extends HttpServlet {
    private IResponseWriter writer = ResponseWriterFactory.createWriter();

    /**
     * @param servletConfig
     * @throws ServletException
     */
    @Override
    public void init(ServletConfig servletConfig) throws ServletException {
    }

    /**
     *
     */
    @Override
    public void destroy() {
    }

    private void initConfig(String configName) throws ServletException {
        ConfigReader config;
        try {
            // load the configuration
            File configFile = new File(configName);
            config = new ConfigReader(new FileReader(configFile));
        } catch (Exception e) {
            throw new ServletException(e);
        }

        AbstractNode.getState().setDatastore(config.getLocalDatastore());
        AbstractNode.getState().setAddress(config.getAddress());
        File f = new File(config.getLocalDatastore() + "/hdt/");
        f.mkdirs();
        f = new File(config.getLocalDatastore() + "/index/");
        f.mkdirs();
    }


    /**
     * @param request
     * @param response
     * @throws ServletException
     */
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String mode = request.getParameter("mode");
        if (mode == null) return;

        try {
            if (mode.equals("setup")) handleSetup(request, response);
            else if (mode.equals("start")) handleStart(request, response);
            else if (mode.equals("performance")) handlePerformance(request, response);
            else if (mode.equals("scalability")) handleScalability(request, response);
            else if (mode.equals("stress")) handleStress(request, response);
            else if (mode.equals("hdt")) handleHdt(request, response);
            else if (mode.equals("community")) handleCommunity(request, response);
            else if (mode.equals("optimize")) handleOptimizer(request, response);
            else if (mode.equals("optimizeStress")) handleOptimizerStress(request, response);
            else if (mode.equals("optimizeLRB")) handleOptimizerLRB(request, response);
            else if (mode.equals("batch")) handleBatch(request, response);
            else if (mode.equals("hdts")) handleHdts(request, response);

            writer.writeRedirect(response.getOutputStream(), request, "experiments");
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private void handleCommunity(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException, NotFoundException, ParserException {
        String oDir = request.getParameter("out");
        String dDir = request.getParameter("data");

        createCommunitiesFromDirectory(dDir, oDir);
    }

    private void handleHdt(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException, NotFoundException, ParserException {
        String oDir = request.getParameter("out");
        String dDir = request.getParameter("data");
        String cDir = request.getParameter("communities");

        createFragmentsFromFile(dDir, oDir, cDir);
    }

    private void createFragmentsFromFile(String file, String oDir, String cDir) throws IOException, NotFoundException {
        RandomString gen = new RandomString(10);
        Random rand = new Random();

        Map<String, Set<Tuple<String, String>>> communityMap = new HashMap<>();
        List<String> communities = new ArrayList<>();

        File[] cDirs = new File(cDir).listFiles();
        System.out.println("Creating communities...");
        for (File c : cDirs) {
            String id = c.getName();
            communities.add(id);
            communityMap.put(id, new HashSet<>());

            String dir = oDir + (oDir.endsWith("/") ? "" : "/") + id;
            new File(dir).mkdirs();
        }

        Set<String> predicates = new HashSet<>();
        System.out.println("Finding predicates...");
        HDT hdt = HDTManager.mapIndexedHDT(file);
        IteratorTripleString iterator = hdt.search("", "", "");
        while (iterator.hasNext()) {
            TripleString triple = iterator.next();
            String pred = triple.getPredicate().toString();
            predicates.add(pred);
        }

        System.out.println("Found " + predicates.size() + " predicates.");

        for (String pred : predicates) {
            System.out.println("Handling predicate " + pred);
            Set<TripleString> tripleSet = new HashSet<>();

            IteratorTripleString it = hdt.search("", pred, "");
            while (it.hasNext()) {
                tripleSet.add(it.next());

                /*if(tripleSet.size() % 1000000 == 0) {
                    String id = gen.nextString();
                    String cid = communities.get(rand.nextInt(communities.size()));
                    String outpath = oDir + (oDir.endsWith("/")? "" : "/") + cid + "/" + id + ".hdt";

                    System.out.println("Saving HDT as " + outpath);

                    HDT newHdt;
                    try {
                        newHdt = HDTManager.generateHDT(new MergedHDTIterator<>(tripleSet), "http://colchain.org/fragments#" + id, new HDTSpecification(), null);
                    } catch (Exception e) {
                        continue;
                    }

                    newHdt.saveToHDT(outpath, null);
                    communityMap.get(cid).add(new Tuple<>(pred, id));

                    tripleSet = new HashSet<>();
                }*/
            }

            if (tripleSet.size() > 0) {
                String id = gen.nextString();
                String cid = communities.get(rand.nextInt(communities.size()));
                String outpath = oDir + (oDir.endsWith("/") ? "" : "/") + cid + "/" + id + ".hdt";

                System.out.println("Saving HDT as " + outpath);

                HDT newHdt;
                try {
                    newHdt = HDTManager.generateHDT(new MergedHDTIterator<>(tripleSet), "http://colchain.org/fragments#" + id, new HDTSpecification(), null);
                } catch (Exception e) {
                    continue;
                }

                newHdt.saveToHDT(outpath, null);
                communityMap.get(cid).add(new Tuple<>(pred, id));
            }
        }

        System.out.println("Writing community fragment files");
        for (String community : communities) {
            String filename = oDir + (oDir.endsWith("/") ? "" : "/") + community + "/fragments";
            FileWriter writer = new FileWriter(filename);
            PrintWriter bWriter = new PrintWriter(writer);

            Set<Tuple<String, String>> set = communityMap.get(community);
            for (Tuple<String, String> tpl : set) {
                String str = tpl.getFirst() + ";" + tpl.getSecond();
                bWriter.println(str);
            }
            bWriter.close();
        }
    }

    void createCommunitiesFromDirectory(String dDir, String oDir) {
        System.out.println("Creating the communities...");
        RandomString gen = new RandomString(10);
        Random rand = new Random();
        List<String> ids = new ArrayList<>();

        for (int i = 0; i < 200; i++) {
            String id = gen.nextString();
            new File(oDir + "/" + id).mkdir();
            ids.add(id);
        }

        int num = 1;
        File[] fFiles = new File(dDir).listFiles();
        for (File fFile : fFiles) {
            if (fFile.getName().contains(".index") || fFile.getName().contains(".chs")) continue;
            int i = rand.nextInt(ids.size());
            String id = ids.get(i);
            System.out.print("\rMoving fragment " + fFile.getName() + " to community " + id + " (" + num + ").");
            num++;
            fFile.renameTo(new File(oDir + "/" + id + "/" + fFile.getName()));
            new File(fFile.getAbsolutePath().replace(".hdt", ".chs"))
                    .renameTo(new File(oDir + "/" + id + "/" + fFile.getName().replace(".hdt", ".chs")));
        }
        System.out.print("\n");
        System.out.println("Done.");
    }

    private void handlePerformance(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String qDir = request.getParameter("queries");
        String oDir = request.getParameter("out");
        int replications = Integer.parseInt(request.getParameter("reps"));

        String outStr = oDir + (oDir.endsWith("/") ? "" : "/") + "reps/";
        File f1 = new File(outStr);
        f1.mkdirs();

        FileWriter out = new FileWriter(outStr + replications + ".csv");
        File dir = new File(qDir);
        File[] qFiles = dir.listFiles();

        System.out.println("Running performance experiments...");

        for (File f : qFiles) {
            System.out.println(f.getName());
            String queryName = f.getName();

            String queryString = FileUtils.readFileToString(f, StandardCharsets.UTF_8);

            final Duration timeout = Duration.ofMinutes(20);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            ExecutorCallable callable = new ExecutorCallable(queryString);

            final Future handler = executor.submit(callable);

            try {
                handler.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

                String str = queryName + ";" + callable.getQet() + ";" + callable.getRsp() + ";"
                        + ColchainJenaConstants.NTB + ";" + ColchainJenaConstants.NEM + ";" + callable.getResults();
                System.out.println(str);
                out.write(str + "\n");
            } catch (Exception e) {
                handler.cancel(true);
                callable.close();

                String str = queryName + ";-1;-1;"
                        + ColchainJenaConstants.NTB + ";" + ColchainJenaConstants.NEM + ";" + callable.getResults();
                System.out.println(str);
                out.write(str + "\n");
            } finally {
                executor.shutdownNow();
            }
        }
        out.close();
    }

    private void runOptimizer(String queryDir, String outStr) throws IOException {
        ColchainJenaConstants.NODES_INVOLVED = new HashSet<>();
        FileWriter out = new FileWriter(outStr + ColchainJenaConstants.NODE + ".csv");
        File dir = new File(queryDir);
        File[] qFiles = dir.listFiles();

        System.out.println("Running optimization experiments...");

        for (File f : qFiles) {
            System.out.println(f.getName());
            String queryName = f.getName();

            String queryString = FileUtils.readFileToString(f, StandardCharsets.UTF_8);

            final Duration timeout = Duration.ofMinutes(20);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            ExecutorCallableOptimizer callable = new ExecutorCallableOptimizer(queryString);

            final Future handler = executor.submit(callable);

            long start = System.currentTimeMillis();
            try {
                handler.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

                String str = queryName + ";" + callable.getOt() + ";"
                        + ColchainJenaConstants.NRFBO + ";" + ColchainJenaConstants.NRF + ";"
                        + ColchainJenaConstants.NRNBO + ";" + ColchainJenaConstants.NRN + ";"
                        + ColchainJenaConstants.NIQ + ";" + ColchainJenaConstants.NODES_INVOLVED.size() + ";"
                        + ColchainJenaConstants.INDEXED + ";" + ColchainJenaConstants.LOCAL;
                System.out.println(str);
                out.write(str + "\n");
            } catch (Exception e) {
                handler.cancel(true);
                callable.close();

                String str;
                if (e instanceof TimeoutException)
                    str = queryName + ";" + callable.getOt() + ";"
                            + ColchainJenaConstants.NRFBO + ";" + ColchainJenaConstants.NRF + ";"
                            + ColchainJenaConstants.NRNBO + ";" + ColchainJenaConstants.NRN + ";"
                            + ColchainJenaConstants.NIQ + ";" + ColchainJenaConstants.NODES_INVOLVED.size() + ";"
                            + ColchainJenaConstants.INDEXED + ";" + ColchainJenaConstants.LOCAL;
                else {
                    e.printStackTrace();
                    str = queryName + ";" + callable.getOt() + ";"
                            + ColchainJenaConstants.NRFBO + ";" + ColchainJenaConstants.NRF + ";"
                            + ColchainJenaConstants.NRNBO + ";" + ColchainJenaConstants.NRN + ";"
                            + ColchainJenaConstants.NIQ + ";" + ColchainJenaConstants.NODES_INVOLVED.size() + ";"
                            + ColchainJenaConstants.INDEXED + ";" + ColchainJenaConstants.LOCAL;
                }
                System.out.println(str);
                out.write(str + "\n");
            } finally {
                executor.shutdownNow();
            }
        }
        out.close();
    }

    private void runScalability(String queryDir, String outStr) throws IOException {
        FileWriter out = new FileWriter(outStr + ColchainJenaConstants.NODE + ".csv");
        File dir = new File(queryDir);
        File[] qFiles = dir.listFiles();

        System.out.println("Running scalability experiments...");

        for (File f : qFiles) {
            System.out.println(f.getName());
            String queryName = f.getName();

            String queryString = FileUtils.readFileToString(f, StandardCharsets.UTF_8);

            final Duration timeout = Duration.ofMinutes(20);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            ExecutorCallable callable = new ExecutorCallable(queryString);

            final Future handler = executor.submit(callable);

            long start = System.currentTimeMillis();
            try {
                handler.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

                String str = queryName + ";" + callable.getQet() + ";" + callable.getRsp() + ";"
                        + ColchainJenaConstants.NTB + ";" + ColchainJenaConstants.NEM + ";"
                        + ColchainJenaConstants.NRF + ";" + ColchainJenaConstants.NRN + ";" + callable.getResults();
                System.out.println(str);
                out.write(str + "\n");
            } catch (Exception e) {
                handler.cancel(true);
                callable.close();

                String str;
                if (e instanceof TimeoutException)
                    str = queryName + ";-1;-1;"
                            + ColchainJenaConstants.NTB + ";" + ColchainJenaConstants.NEM + ";"
                            + ColchainJenaConstants.NRF + ";" + ColchainJenaConstants.NRN + ";" + callable.getResults();
                else {
                    e.printStackTrace();
                    long time = System.currentTimeMillis() - start;
                    str = queryName + ";" + time + ";" + time + ";"
                            + ColchainJenaConstants.NTB + ";" + ColchainJenaConstants.NEM + ";"
                            + ColchainJenaConstants.NRF + ";" + ColchainJenaConstants.NRN + ";" + callable.getResults();
                }
                System.out.println(str);
                out.write(str + "\n");
            } finally {
                executor.shutdownNow();
            }
        }
        out.close();
    }

    private void handleScalability(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String qDir = request.getParameter("queries");
        String oDir = request.getParameter("out");
        String load = request.getParameter("load");
        int nodes = Integer.parseInt(request.getParameter("nodes"));

        String queryDir = qDir + (qDir.endsWith("/") ? "" : "/") + "client" + ColchainJenaConstants.NODE + "/" + load;
        String outStr = oDir + (oDir.endsWith("/") ? "" : "/") + "scalability/" + nodes + "/" + load + "/";
        File f1 = new File(outStr);
        f1.mkdirs();

        runScalability(queryDir, outStr);
    }

    private void handleOptimizer(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String qDir = request.getParameter("queries");
        String oDir = request.getParameter("out");
        String load = request.getParameter("load");
        int nodes = Integer.parseInt(request.getParameter("nodes"));

        String queryDir = qDir + (qDir.endsWith("/") ? "" : "/") + "client" + ColchainJenaConstants.NODE + "/" + load;
        String outStr = oDir + (oDir.endsWith("/") ? "" : "/") + "optimizer/" + nodes + "/" + load + "/";
        File f1 = new File(outStr);
        f1.mkdirs();

        runOptimizer(queryDir, outStr);
    }

    private void handleOptimizerStress(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String qDir = request.getParameter("queries");
        String oDir = request.getParameter("out");
        int nodes = Integer.parseInt(request.getParameter("nodes"));

        String queryDir = qDir + (qDir.endsWith("/") ? "" : "/") + "client" + ColchainJenaConstants.NODE;
        String outStr = oDir + (oDir.endsWith("/") ? "" : "/") + "optimizer/" + nodes + "/sts/";
        File f1 = new File(outStr);
        f1.mkdirs();

        runOptimizer(queryDir, outStr);
    }

    private void handleOptimizerLRB(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String qDir = request.getParameter("queries");
        String oDir = request.getParameter("out");

        //String queryDir = qDir + (qDir.endsWith("/") ? "" : "/") + "client" + LothbrokJenaConstants.NODE + "/" + load;
        String outStr = oDir + (oDir.endsWith("/") ? "" : "/") + "optimizer/1/largerdfbench/";
        File f1 = new File(outStr);
        f1.mkdirs();

        runOptimizer(qDir, outStr);
    }

    private void handleStress(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String qDir = request.getParameter("queries");
        String oDir = request.getParameter("out");
        int nodes = Integer.parseInt(request.getParameter("nodes"));

        String queryDir = qDir + (qDir.endsWith("/") ? "" : "/") + "client" + ColchainJenaConstants.NODE;
        String outStr = oDir + (oDir.endsWith("/") ? "" : "/") + "scalability/" + nodes + "/sts/";
        File f1 = new File(outStr);
        f1.mkdirs();

        runScalability(queryDir, outStr);
    }

    private void handleSetup(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String config = request.getParameter("config");
        initConfig(config);
        WebInterfaceServlet.INIT = true;

        int nodes = Integer.parseInt(request.getParameter("nodes"));
        int reps = Integer.parseInt(request.getParameter("rep"));

        String dirname = request.getParameter("dir");
        File dir = new File(dirname);
        if (!dir.exists() || !dir.isDirectory()) return;
        File[] fDirs = dir.listFiles();

        File oDir = new File("setup/");
        oDir.mkdirs();

        // Create Node IDs
        Gson gson = new Gson();
        List<String> ids = new ArrayList<>();
        RandomString rs = new RandomString(10);
        for (int i = 0; i < nodes; i++) {
            ids.add(rs.nextString());
        }
        FileWriter out = new FileWriter("setup/ids");
        out.write(gson.toJson(ids));
        out.close();

        new File("setup/hdt").mkdirs();
        new File("setup/updates").mkdirs();

        // Create community distribution
        Random rand = new Random();
        Map<String, Tuple<Integer, Set<Integer>>> map = new HashMap<>();
        out = new FileWriter("setup/distribution");
        int j = 0, k = 0;
        for (File fDir : fDirs) {
            System.out.println(j + "/" + fDirs.length);
            j++;
            if (!fDir.isDirectory()) continue;
            String cid = fDir.getName();
            //int owner = k;
            //Set<Integer> set = new HashSet<>();
            //set.add(k);
            int owner = -1;
            int num;
            if (reps == 0)
                num = (int) Math.ceil(128.0 / (double) j);
            else
                num = reps;
            if (num == 0) num = 1;
            Set<Integer> set = new HashSet<>();
            while (set.size() < num) {
                int next = rand.nextInt(nodes);
                set.add(next);
                if (owner == -1) owner = next;
            }
            map.put(cid, new Tuple<>(owner, set));
            k++;


            // Create updates to fragments
            /*BufferedReader reader = new BufferedReader(new FileReader(fDir.getAbsolutePath() + "/fragments"));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.equals("")) continue;
                String[] ws = line.split(";");
                System.out.println(ws[0]);

                for (int i = 1; i <= 100; i = i * 10) {
                    ChainEntry entry = ChainEntry.getInitialEntry();
                    Set<Triple> added = new HashSet<>();

                    for (int k = 1; k <= i; k++) {
                        List<Operation> operations = getOperations(ws[0], added);
                        for (Operation op : operations) {
                            if (op.getType() == Operation.OperationType.ADD)
                                added.add(op.getTriple());
                            else
                                added.remove(op.getTriple());
                        }

                        ITransaction transaction = TransactionFactory.getTransaction(operations, ws[1], ids.get(rand.nextInt(ids.size())), rs.nextString(), k);
                        entry = new ChainEntry(transaction, entry);
                    }

                    String dname = "setup/updates/" + ws[1];
                    new File(dname).mkdirs();
                    FileWriter fout = new FileWriter(dname + "/" + i);
                    fout.write(gson.toJson(entry));
                    fout.close();
                }
            }*/
        }

        out.write(gson.toJson(map));
        out.flush();
        out.close();

        // Create keys
        File prDir = new File("setup/keys/private/");
        prDir.mkdirs();
        File puDir = new File("setup/keys/public/");
        puDir.mkdirs();

        for (int i = 0; i < nodes; i++) {
            KeyPair kp = CryptoUtils.generateKeyPair();

            byte[] priv = kp.getPrivate().getEncoded();
            byte[] pub = kp.getPublic().getEncoded();

            OutputStream os = new FileOutputStream("setup/keys/private/" + i);
            os.write(priv);
            os.close();

            os = new FileOutputStream("setup/keys/public/" + i);
            os.write(pub);
            os.close();
        }
    }

    private void handleStart(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String config = request.getParameter("config");
        initConfig(config);
        WebInterfaceServlet.INIT = true;
        String setup = request.getParameter("setup");
        String dirname = request.getParameter("dir");
        int node = Integer.parseInt(request.getParameter("id"));
        int nodes = Integer.parseInt(request.getParameter("nodes"));
        ColchainJenaConstants.NODES = nodes;
        ColchainJenaConstants.NODE = node;
        int chain;
        String ch = request.getParameter("chain");
        if (ch == null || ch.equals(""))
            chain = 0;
        else
            chain = Integer.parseInt(request.getParameter("chain"));
        AbstractNode.getState().setAddress("http://172.21.233.15:3" + String.format("%03d", node));
        setup = setup + (setup.endsWith("/") ? "" : "/");

        if (node < nodes) {
            PrivateKey pKey = CryptoUtils.getPrivateKey(FileUtils.readFileToByteArray(new File(setup + "keys/private/" + node)));
            PublicKey puKey = CryptoUtils.getPublicKey(FileUtils.readFileToByteArray(new File(setup + "keys/public/" + node)));
            KeyPair keys = new KeyPair(puKey, pKey);
            AbstractNode.getState().setKeyPair(keys);
        }

        String file = setup + "distribution";
        String json = readFile(file);
        Gson gson = new Gson();
        Type type = new TypeToken<Map<String, Tuple<Integer, Set<Integer>>>>() {
        }.getType();
        Map<String, Tuple<Integer, Set<Integer>>> map = gson.fromJson(json, type);

        json = readFile(setup + "ids");
        type = new TypeToken<List<String>>() {
        }.getType();
        List<String> ids = gson.fromJson(json, type);
        try {
            AbstractNode.getState().setId(ids.get(node));
        } catch (IndexOutOfBoundsException e) {

        }

        int cnum = 1;

        for (String s : map.keySet()) {
            Tuple<Integer, Set<Integer>> tpl = map.get(s);
            Set<Integer> set = tpl.getSecond();
            int owner = tpl.getFirst();
            byte[] oKey = FileUtils.readFileToByteArray(new File(setup + "keys/public/" + owner));

            Set<CommunityMember> participants = new HashSet<>();
            Set<CommunityMember> observers = new HashSet<>();
            for (int i = 0; i < nodes; i++) {
                if (set.contains(i)) {
                    participants.add(new CommunityMember(ids.get(i), "http://172.21.233.15:3" + String.format("%03d", i)));
                    //participants.add(new CommunityMember(ids.get(i), "http://172.21.233.15:808" + i + "/kc"));
                } else {
                    observers.add(new CommunityMember(ids.get(i), "http://172.21.233.15:3" + String.format("%03d", i)));
                    //observers.add(new CommunityMember(ids.get(i), "http://172.21.233.15:808" + i + "/kc"));
                }
            }
            boolean participant;
            Community.MemberType mt;
            if (set.contains(node)) {
                participant = true;
                mt = Community.MemberType.PARTICIPANT;
            } else {
                participant = false;
                mt = Community.MemberType.OBSERVER;
            }

            Community c = new Community(s, "Community " + cnum, mt, participants, observers);
            cnum++;
            AbstractNode.getState().addCommunity(c);

            String fFile = dirname + (dirname.endsWith("/") ? "" : "/") + s + "/fragments";
            BufferedReader reader = new BufferedReader(new FileReader(fFile));
            String line = reader.readLine();
            while (line != null) {
                if (line.equals("")) {
                    line = reader.readLine();
                    continue;
                }
                String[] ws = line.split(";");
                String path = fFile.replace("/fragments", "/" + ws[1] + ".hdt");

                if (participant) {
                    if (chain > 0) {
                        String filename = setup + "updates/" + ws[1] + "/" + chain;
                        String j = FileUtils.readFileToString(new File(filename), StandardCharsets.UTF_8);

                        Gson g = new GsonBuilder().registerTypeAdapter(ChainEntry.class, new ChainSerializer()).create();
                        ChainEntry entry = g.fromJson(j, ChainEntry.class);

                        AbstractNode.getState().addNewFragment(ws[1], ws[0], path, s, oKey, entry);
                    } else {
                        AbstractNode.getState().addNewFragment(ws[1], ws[0], path, s, oKey);
                    }
                } else {
                    AbstractNode.getState().addNewObservedFragment(ws[1], ws[0], s, oKey);
                }

                line = reader.readLine();
            }
            reader.close();
        }
    }

    private void handleBatch(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException, NotFoundException, ParserException {
        String batch = request.getParameter("batch");
        String oDir = request.getParameter("out");

        createCommunitiesFromBatch(batch, oDir);
    }

    void createCommunitiesFromBatch(String batch, String oDir) throws IOException, NotFoundException, ParserException {
        System.out.println();

        File batchDir = new File(batch);
        for (File f : batchDir.listFiles()) {
            String name = f.getName().replace(".nq", "");
            System.out.println(name);

            HDT hdt = HDTManager.generateHDT(f.getAbsolutePath(), "http://relweb.cs.aau.dk/lothbrok", RDFNotation.NQUAD, new HDTSpecification(), ProgressOut.getInstance());

            new File(oDir + "/" + name).mkdirs();
            createFragmentsFromHDT(hdt, oDir + "/" + name);
        }
    }

    private void createFragmentsFromHDT(HDT hdt, String oDir) throws IOException, NotFoundException {
        RandomString gen = new RandomString(10);
        Set<Tuple<String, String>> preds = new HashSet<>();

        Set<String> predicates = new HashSet<>();
        System.out.println("Finding predicates...");
        IteratorTripleString iterator = hdt.search("", "", "");
        while (iterator.hasNext()) {
            TripleString triple = iterator.next();
            String pred = triple.getPredicate().toString();
            predicates.add(pred);
        }

        System.out.println("Found " + predicates.size() + " predicates.");

        for (String pred : predicates) {
            System.out.println("Handling predicate " + pred);
            Set<TripleString> tripleSet = new HashSet<>();

            IteratorTripleString it = hdt.search("", pred, "");
            while (it.hasNext()) {
                tripleSet.add(it.next());

                /*if(tripleSet.size() % 1000000 == 0) {
                    String id = gen.nextString();
                    String cid = communities.get(rand.nextInt(communities.size()));
                    String outpath = oDir + (oDir.endsWith("/")? "" : "/") + cid + "/" + id + ".hdt";

                    System.out.println("Saving HDT as " + outpath);

                    HDT newHdt;
                    try {
                        newHdt = HDTManager.generateHDT(new MergedHDTIterator<>(tripleSet), "http://colchain.org/fragments#" + id, new HDTSpecification(), null);
                    } catch (Exception e) {
                        continue;
                    }

                    newHdt.saveToHDT(outpath, null);
                    communityMap.get(cid).add(new Tuple<>(pred, id));

                    tripleSet = new HashSet<>();
                }*/
            }

            if (tripleSet.size() > 0) {
                String id = gen.nextString();
                String outpath = oDir + (oDir.endsWith("/") ? "" : "/") + id + ".hdt";

                System.out.println("Saving HDT as " + outpath);

                HDT newHdt;
                try {
                    newHdt = HDTManager.generateHDT(new MergedHDTIterator<>(tripleSet), "http://colchain.org/fragments#" + id, new HDTSpecification(), null);
                } catch (Exception e) {
                    continue;
                }

                newHdt.saveToHDT(outpath, null);
                preds.add(new Tuple<>(pred, id));
            }
        }

        System.out.println("Writing community fragment files");
        String filename = oDir + (oDir.endsWith("/") ? "" : "/") + "/fragments";
        FileWriter writer = new FileWriter(filename);
        PrintWriter bWriter = new PrintWriter(writer);

        for (Tuple<String, String> tpl : preds) {
            String str = tpl.getFirst() + ";" + tpl.getSecond();
            bWriter.println(str);
        }
        bWriter.close();
    }

    private void handleHdts(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException, NotFoundException, ParserException {
        int num = Integer.parseInt(request.getParameter("num"));
        String oDir = request.getParameter("out");

        String dir = request.getParameter("dir");
        File d = new File(dir);
        List<HDT> hdts = new ArrayList<>();
        for(File f : d.listFiles()) {
            if(f.getName().contains(".index")) continue;
            hdts.add(HDTManager.mapIndexedHDT(f.getAbsolutePath()));
        }

        createFragmentsFromHDTs(hdts, oDir, num);
    }

    private void createFragmentsFromHDTs(List<HDT> hdts, String oDir, int communities) throws IOException, NotFoundException {
        RandomString gen = new RandomString(10);
        Random rand = new Random();
        Map<String, Set<Tuple<String, String>>> preds = new HashMap<>();

        System.out.println("Generating communities...");
        List<String> cids = new ArrayList<>();
        for(int i = 0; i < communities; i++) {
            String cid = gen.nextString();
            cids.add(cid);
            new File(oDir + (oDir.endsWith("/") ? "" : "/") + cid).mkdirs();
            preds.put(cid, new HashSet<>());
        }

        Set<String> predicates = new HashSet<>();
        System.out.println("Finding predicates...");
        for(HDT hdt : hdts) {
            IteratorTripleString iterator = hdt.search("", "", "");
            while (iterator.hasNext()) {
                TripleString triple = iterator.next();
                String pred = triple.getPredicate().toString();
                predicates.add(pred);
            }
        }

        System.out.println("Found " + predicates.size() + " predicates.");

        for (String pred : predicates) {
            System.out.println("Handling predicate " + pred);
            Set<TripleString> tripleSet = new HashSet<>();

            for(HDT hdt : hdts) {
                IteratorTripleString it = hdt.search("", pred, "");
                while (it.hasNext()) {
                    tripleSet.add(it.next());

                /*if(tripleSet.size() % 1000000 == 0) {
                    String id = gen.nextString();
                    String cid = communities.get(rand.nextInt(communities.size()));
                    String outpath = oDir + (oDir.endsWith("/")? "" : "/") + cid + "/" + id + ".hdt";

                    System.out.println("Saving HDT as " + outpath);

                    HDT newHdt;
                    try {
                        newHdt = HDTManager.generateHDT(new MergedHDTIterator<>(tripleSet), "http://colchain.org/fragments#" + id, new HDTSpecification(), null);
                    } catch (Exception e) {
                        continue;
                    }

                    newHdt.saveToHDT(outpath, null);
                    communityMap.get(cid).add(new Tuple<>(pred, id));

                    tripleSet = new HashSet<>();
                }*/
                }
            }

            if (tripleSet.size() > 0) {
                String id = gen.nextString();
                String cid = cids.get(rand.nextInt(cids.size()));
                String outpath = oDir + (oDir.endsWith("/") ? "" : "/") + cid + "/" + id + ".hdt";

                System.out.println("Saving HDT as " + outpath);

                HDT newHdt;
                try {
                    newHdt = HDTManager.generateHDT(new MergedHDTIterator<>(tripleSet), "http://colchain.org/fragments#" + id, new HDTSpecification(), null);
                } catch (Exception e) {
                    continue;
                }

                newHdt.saveToHDT(outpath, null);
                preds.get(cid).add(new Tuple<>(pred, id));
            }
        }

        System.out.println("Writing community fragment files");

        for(String cid : preds.keySet()) {
            String filename = oDir + (oDir.endsWith("/") ? "" : "/") + cid + "/fragments";
            FileWriter writer = new FileWriter(filename);
            PrintWriter bWriter = new PrintWriter(writer);
            for(Tuple<String, String> tpl : preds.get(cid)) {
                String str = tpl.getFirst() + ";" + tpl.getSecond();
                bWriter.println(str);
            }
            bWriter.close();
        }
    }

    private static String readFile(String filePath) {
        StringBuilder contentBuilder = new StringBuilder();

        try (Stream<String> stream = Files.lines(Paths.get(filePath), StandardCharsets.UTF_8)) {
            stream.forEach(s -> contentBuilder.append(s).append("\n"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return contentBuilder.toString();
    }

    private class ExecutorCallable implements Callable<String> {
        private final QueryExecution qExecutor;
        private long rsp = 0;
        private long qet = 0;
        private int results = 0;

        public ExecutorCallable(String qStr) {
            Query query = QueryFactory.create(qStr);
            final ColchainGraph graph = new ColchainGraph();
            Model model = ModelFactory.createModelForGraph(graph);
            qExecutor = QueryExecutionFactory.create(query, model);
        }

        public long getRsp() {
            return rsp;
        }

        public long getQet() {
            return qet;
        }

        public int getResults() {
            return results;
        }

        public void close() {
            qExecutor.abort();
        }

        @Override
        public String call() throws Exception {
            final ResultSet rs = qExecutor.execSelect();
            long start = System.currentTimeMillis();
            boolean first = true;

            while (rs.hasNext()) {
                rs.next();
                results++;
                if (first) {
                    rsp = System.currentTimeMillis() - start;
                    first = false;
                }
            }

            qet = System.currentTimeMillis() - start;
            return "";
        }
    }

    private class ExecutorCallableOptimizer implements Callable<Long> {
        private final QueryExecution qExecutor;
        private long ot = 0;
        private int results = 0;

        public ExecutorCallableOptimizer(String qStr) {
            Query query = QueryFactory.create(qStr);
            final ColchainGraph graph = new ColchainGraph();
            Model model = ModelFactory.createModelForGraph(graph);
            qExecutor = QueryExecutionFactory.create(query, model);
        }


        public long getOt() {
            return ot;
        }

        public void close() {
            qExecutor.abort();
        }

        @Override
        public Long call() throws Exception {
            long start = System.currentTimeMillis();
            final ResultSet rs = qExecutor.execSelect();
            ot = System.currentTimeMillis() - start;
            return ot;
        }
    }
}
