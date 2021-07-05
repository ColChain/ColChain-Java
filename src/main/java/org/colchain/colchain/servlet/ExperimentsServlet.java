package org.colchain.colchain.servlet;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
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
import org.colchain.colchain.util.ChainSerializer;
import org.colchain.colchain.util.ConfigReader;
import org.colchain.colchain.util.CryptoUtils;
import org.colchain.colchain.util.RandomString;
import org.colchain.colchain.writer.IResponseWriter;
import org.colchain.colchain.writer.ResponseWriterFactory;
import org.apache.commons.io.FileUtils;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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

        if (mode.equals("setup")) handleSetup(request, response);
        else if (mode.equals("start")) handleStart(request, response);
        else if (mode.equals("performance")) handlePerformance(request, response);
        else if (mode.equals("versioning")) handleVersioning(request, response);
        else if (mode.equals("updates")) handleUpdates(request, response);
        else if (mode.equals("dbpedia")) handleDbpedia(request, response);

        try {
            writer.writeRedirect(response.getOutputStream(), request, "experiments");
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private void handleUpdates(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String oDir = request.getParameter("out");
        String uDir = request.getParameter("updates");
        uDir = uDir + (uDir.endsWith("/") ? "" : "/");
        int length = Integer.parseInt(request.getParameter("length"));

        String outStr = oDir + (oDir.endsWith("/") ? "" : "/") + "updates/";
        File f1 = new File(outStr);
        f1.mkdirs();

        Gson g = new GsonBuilder().registerTypeAdapter(ChainEntry.class, new ChainSerializer()).create();
        FileWriter out = new FileWriter(outStr + length + ".csv");
        System.out.println("Running updates experiments...");

        Set<String> fids = AbstractNode.getState().getDatasourceIds();

        for (String fid : fids) {
            File f = new File(uDir + fid + "/" + length);
            String json = FileUtils.readFileToString(f, StandardCharsets.UTF_8);

            ChainEntry entry = g.fromJson(json, ChainEntry.class);
            KnowledgeChain chain = AbstractNode.getState().getChain(fid);

            final Duration timeout = Duration.ofMinutes(30);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            UpdaterCallable callable = new UpdaterCallable(entry, chain);
            final Future handler = executor.submit(callable);

            try {
                handler.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

                String str = fid + ";" + callable.getTime() + ";" + callable.getSize();
                System.out.println(str);
                out.write(str + "\n");
            } catch (Exception e) {
                handler.cancel(true);
            } finally {
                executor.shutdownNow();
            }
        }

        out.close();
    }

    private ITransaction getFirst(ChainEntry entry) {
        ChainEntry e = entry;
        if (e.isFirst()) return null;
        while (!e.previous().isFirst()) {
            e = e.previous();
        }
        return e.getTransaction();
    }

    private ChainEntry removeFirst(ChainEntry entry) {
        if (entry.isFirst() || entry.previous().isFirst()) return ChainEntry.getInitialEntry();
        return new ChainEntry(entry.getTransaction(), removeFirst(entry.previous()));
    }

    private void handleVersioning(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String qDir = request.getParameter("queries");
        String oDir = request.getParameter("out");
        int length = Integer.parseInt(request.getParameter("length"));

        String outStr = oDir + (oDir.endsWith("/") ? "" : "/") + "versioning/";
        File f1 = new File(outStr);
        f1.mkdirs();

        FileWriter out = new FileWriter(outStr + length + ".csv");
        File dir = new File(qDir);
        File[] qFiles = dir.listFiles();

        System.out.println("Running versioning experiments...");

        for (File f : qFiles) {
            System.out.println(f.getName());
            String queryName = f.getName();

            String queryString = FileUtils.readFileToString(f, StandardCharsets.UTF_8);

            final Duration timeout = Duration.ofMinutes(20);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            ExecutorCallable callable = new ExecutorCallable(queryString, 0);

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
        int j = 0;
        for (File fDir : fDirs) {
            System.out.println(j + "/" + fDirs.length);
            j++;
            if (!fDir.isDirectory()) continue;
            String cid = fDir.getName();
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

    private List<Operation> getOperations(String predicate, Set<Triple> added) {
        List<Operation> ops = new ArrayList<>();
        Random rnd = new Random();
        int num = rnd.nextInt(1000);
        List<Triple> cpy = new ArrayList<>(added);
        Set<Triple> newtpls = new HashSet<>();

        for (int i = 0; i < num; i++) {
            if (cpy.size() > 0) {
                int rnum = rnd.nextInt(2);
                if (rnum == 0) {
                    Triple tpl = cpy.get(rnd.nextInt(cpy.size()));
                    cpy.remove(tpl);

                    ops.add(new Operation(Operation.OperationType.DEL, tpl));
                    continue;
                }
            }

            int onum = 1;
            Triple tpl = new Triple("http://colchain.org/subject", predicate, "http://colchain.org/object" + onum);
            while (added.contains(tpl) || newtpls.contains(tpl)) {
                onum++;
                tpl = new Triple("http://colchain.org/subject", predicate, "http://colchain.org/object" + onum);
            }
            newtpls.add(tpl);

            ops.add(new Operation(Operation.OperationType.ADD, tpl));
        }

        return ops;
    }

    private void handleStart(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String config = request.getParameter("config");
        initConfig(config);
        WebInterfaceServlet.INIT = true;
        String setup = request.getParameter("setup");
        String dirname = request.getParameter("dir");
        int node = Integer.parseInt(request.getParameter("id"));
        int nodes = Integer.parseInt(request.getParameter("nodes"));
        int chain;
        String ch = request.getParameter("chain");
        if (ch == null || ch.equals(""))
            chain = 0;
        else
            chain = Integer.parseInt(request.getParameter("chain"));
        AbstractNode.getState().setAddress("http://172.21.232.208:3" + String.format("%03d", node));
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
                    participants.add(new CommunityMember(ids.get(i), "http://172.21.232.208:3" + String.format("%03d", i)));
                } else {
                    observers.add(new CommunityMember(ids.get(i), "http://172.21.232.208:3" + String.format("%03d", i)));
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

    private void handleDbpedia(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String config = request.getParameter("config");
        initConfig(config);
        WebInterfaceServlet.INIT = true;
        String setup = request.getParameter("setup");
        String dirname = request.getParameter("dir");
        int node = Integer.parseInt(request.getParameter("id"));
        int nodes = Integer.parseInt(request.getParameter("nodes"));
        //AbstractNode.getState().setAddress("http://172.21.232.208:3" + String.format("%03d", node));
        //AbstractNode.getState().setAddress("http://localhost:8080/colchain-0.1");
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
                    //participants.add(new CommunityMember(ids.get(i), "http://172.21.232.208:3" + String.format("%03d", i)));
                    participants.add(new CommunityMember(ids.get(i), "http://172.21.232.208:808" + i + "/kc"));
                } else {
                    //observers.add(new CommunityMember(ids.get(i), "http://172.21.232.208:3" + String.format("%03d", i)));
                    observers.add(new CommunityMember(ids.get(i), "http://172.21.232.208:808" + i + "/kc"));
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
                    String filename = setup + "updates/" + ws[1];
                    String j = FileUtils.readFileToString(new File(filename), StandardCharsets.UTF_8);

                    Gson g = new GsonBuilder().registerTypeAdapter(ChainEntry.class, new ChainSerializer()).create();
                    ChainEntry entry = g.fromJson(j, ChainEntry.class);

                    AbstractNode.getState().addNewFragment(ws[1], ws[0], path, s, oKey, entry);
                } else {
                    AbstractNode.getState().addNewObservedFragment(ws[1], ws[0], s, oKey);
                }

                line = reader.readLine();
            }
            reader.close();
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

        public ExecutorCallable(String qStr, long timestamp) {
            Query query = QueryFactory.create(qStr);
            final ColchainGraph graph = new ColchainGraph(timestamp);
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

    private class UpdaterCallable implements Callable<String> {
        private ChainEntry entry;
        private KnowledgeChain chain;
        private long size = 0;
        private long time = 0;

        public UpdaterCallable(ChainEntry entry, KnowledgeChain chain) {
            this.entry = entry;
            this.chain = chain;
        }

        public long getSize() {
            return size;
        }

        public long getTime() {
            return time;
        }

        @Override
        public String call() throws Exception {
            long start = System.currentTimeMillis();
            while (!entry.isFirst()) {
                ITransaction transaction = getFirst(entry);
                chain.transition(transaction);
                entry = removeFirst(entry);
            }
            time = System.currentTimeMillis() - start;
            size = chain.size();
            return "";
        }
    }
}
