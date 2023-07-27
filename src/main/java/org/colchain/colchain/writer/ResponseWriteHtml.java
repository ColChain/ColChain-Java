package org.colchain.colchain.writer;

import com.google.gson.Gson;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import org.apache.commons.lang3.StringEscapeUtils;
import org.colchain.colchain.community.CommunityMember;
import org.colchain.colchain.knowledgechain.impl.ChainEntry;
import org.colchain.colchain.knowledgechain.impl.KnowledgeChain;
import org.colchain.colchain.transaction.Operation;
import org.colchain.index.graph.IGraph;
import org.colchain.index.util.Triple;
import org.colchain.index.util.Tuple;
import org.colchain.colchain.community.Community;
import org.colchain.colchain.node.AbstractNode;
import org.colchain.colchain.sparql.graph.ColchainGraph;
import org.colchain.colchain.transaction.ITransaction;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.linkeddatafragments.datasource.IDataSource;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ResponseWriteHtml implements IResponseWriter {
    private final Configuration cfg;
    private boolean error = false;

    private Template indexTemplate;
    private Template queryTemplate;
    private Template setupTemplate;
    private Template searchTemplate;
    private Template communityTemplate;
    private Template modifyTemplate;
    private Template updateTemplate;
    private Template fragmentTemplate;
    private Template transactionTemplate;
    private Template predicateTemplate;

    public ResponseWriteHtml() {
        cfg = new Configuration(Configuration.VERSION_2_3_22);
        cfg.setClassForTemplateLoading(getClass(), "/views");
        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        try {
            indexTemplate = cfg.getTemplate("index.ftl.html");
            queryTemplate = cfg.getTemplate("query.ftl.html");
            setupTemplate = cfg.getTemplate("setup.ftl.html");
            searchTemplate = cfg.getTemplate("search.ftl.html");
            communityTemplate = cfg.getTemplate("community.ftl.html");
            modifyTemplate = cfg.getTemplate("modify.ftl.html");
            updateTemplate = cfg.getTemplate("update.ftl.html");
            fragmentTemplate = cfg.getTemplate("fragment.ftl.html");
            transactionTemplate = cfg.getTemplate("transaction.ftl.html");
            predicateTemplate = cfg.getTemplate("predicate.ftl.html");
        } catch (IOException e) {
            System.out.println(e.getMessage());
            error = true;
        }
    }

    @Override
    public void writeNotInitiated(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        if (error) {
            outputStream.println("There was an error. Please restart the client.");
            return;
        }
        String uri = request.getRequestURI();
        if (uri.contains("ldf"))
            uri = uri.replace(uri.substring(uri.indexOf("ldf/")), "");
        else if (uri.contains("api"))
            uri = uri.replace(uri.substring(uri.indexOf("api/")), "");
        outputStream.println(
                "<!doctype html>\n" +
                        "<html>\n" +
                        "<head>\n" +
                        "<meta charset=\"utf-8\">\n" +
                        "<title>Not initiated</title>\n" +
                        "</head>\n" +
                        "<body>\n" +
                        "  The client has not been initiated! Please go to <a href=\"" + uri + "\">this link</a> to initiate. \n" +
                        "</body>\n" +
                        "</html>"
        );
    }

    @Override
    public void writeLandingPage(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        if (error) {
            outputStream.println("There was an error. Please restart the client.");
            return;
        }
        String uri = request.getRequestURI();
        if (!uri.endsWith("/"))
            uri += "/";

        Map data = new HashMap();

        data.put("assetsPath", "/assets/");
        data.put("date", new Date());
        data.put("requestUri", uri);
        data.put("page", "Home");
        data.put("updates", AbstractNode.getState().getPendingUpdates());
        data.put("numUpdates", AbstractNode.getState().getPendingUpdates().size());

        int participant = 0, observer = 0, largest = 0, smallest = 0, most = 0, least = 0;
        List<Community> communities = AbstractNode.getState().getCommunities();
        List<Community> participating = new ArrayList<>();
        List<Community> observing = new ArrayList<>();

        List<CommunityMember> nodes = new ArrayList<>();
        Set<String> added = new HashSet<>();
        CommunityMember cm = new CommunityMember(AbstractNode.getState().getId(), AbstractNode.getState().getAddress());
        added.add(AbstractNode.getState().getId());
        List<Tuple<String, String>> partNodes = new ArrayList<>();
        List<Tuple<String, String>> obsNodes = new ArrayList<>();

        for (Community c : communities) {
            if (c.getMemberType() == Community.MemberType.PARTICIPANT) {
                participant++;
                participating.add(c);
            }
            else if (c.getMemberType() == Community.MemberType.OBSERVER) {
                observer++;
                observing.add(c);
            }

            int cnt = c.getParticipants().size();
            if (smallest == 0 || cnt < smallest) smallest = cnt;
            if (cnt > largest) largest = cnt;

            cnt = c.getFragmentIds().size();
            if (least == 0 || cnt < least) least = cnt;
            if (cnt > most) most = cnt;

            for(CommunityMember mem : c.getParticipants()) {
                if(!added.contains(mem.getId())) {
                    added.add(mem.getId());
                    nodes.add(mem);
                }
                partNodes.add(new Tuple<>(mem.getId(), c.getId()));
            }

            for(CommunityMember mem : c.getObservers()) {
                if(!added.contains(mem.getId())) {
                    added.add(mem.getId());
                    nodes.add(mem);
                }
                obsNodes.add(new Tuple<>(mem.getId(), c.getId()));
            }
        }

        int stored = AbstractNode.getState().getChains().size();
        int indexed = AbstractNode.getState().getIndex().getGraphs().size() - stored;

        data.put("participant", participant);
        data.put("observer", observer);

        boolean ptc = false;
        boolean obs = false;
        if(participant == 0) ptc = true;
        if(observer == 0) obs = true;
        data.put("ptc", ptc);
        data.put("obs", obs);

        data.put("stored", stored);
        data.put("indexed", Integer.toString(indexed));

        boolean strd = false;
        boolean idx = false;
        if(stored == 0) strd = true;
        if(indexed == 0) idx = true;
        data.put("strd", strd);
        data.put("idx", idx);

        data.put("id", AbstractNode.getState().getId());
        data.put("address", AbstractNode.getState().getAddressPath());
        data.put("datastore", AbstractNode.getState().getDatastore());
        data.put("largest", largest);
        data.put("smallest", smallest);
        data.put("most", most);
        data.put("least", least);
        data.put("participating", participating);
        data.put("observing", observing);
        data.put("nodes", nodes);
        data.put("communities", communities);
        data.put("participants", partNodes);
        data.put("observers", obsNodes);
        data.put("local", cm);

        String date = "";
        boolean hasDate = false;
        if(request.getParameter("date") != null && !request.getParameter("date").equals("")) {
            date = request.getParameter("date");
            hasDate = true;
        }

        data.put("day", date);
        data.put("hasDate", hasDate);

        indexTemplate.process(data, new OutputStreamWriter(outputStream));
    }

    @Override
    public void writeRedirect(ServletOutputStream outputStream, HttpServletRequest request, String path) throws Exception {
        String uri = request.getRequestURI();
        uri = uri.substring(0, uri.indexOf(path));
        outputStream.println(
                "<!doctype html>\n" +
                        "<html>\n" +
                        "<head>\n" +
                        "<meta http-equiv=\"refresh\" content=\"0; url='" + uri + "\">\n" +
                        "</head>\n" +
                        "<body>\n" +
                        "</body>\n" +
                        "</html>"
        );
    }

    @Override
    public void writeSearch(ServletOutputStream outputStream, HttpServletRequest request, Set<Tuple<String, Tuple<String, String>>> comms) throws Exception {
        String uri = request.getRequestURI().replace("/api/community", "");

        Map data = new HashMap();

        data.put("assetsPath", "../assets/");
        data.put("date", new Date());
        data.put("requestUri", uri);
        data.put("address", AbstractNode.getState().getAddressPath());
        data.put("page", "Community search");
        data.put("updates", AbstractNode.getState().getPendingUpdates());
        data.put("numUpdates", AbstractNode.getState().getPendingUpdates().size());

        List<List<String>> cs = new ArrayList<>();
        for (Tuple<String, Tuple<String, String>> tpl : comms) {
            List<String> eles = new ArrayList<>();
            eles.add(tpl.getFirst());
            eles.add(tpl.getSecond().getFirst());
            eles.add(tpl.getSecond().getSecond());
            cs.add(eles);
        }

        data.put("numFound", cs.size());
        data.put("communities", cs);

        searchTemplate.process(data, new OutputStreamWriter(outputStream));
    }

    @Override
    public void writeInit(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        String uri = request.getRequestURI();

        Map data = new HashMap();

        data.put("assetsPath", "assets/");
        data.put("date", new Date());
        data.put("requestUri", uri);
        data.put("address", AbstractNode.getState().getAddressPath());
        data.put("page", "Setup");

        setupTemplate.process(data, new OutputStreamWriter(outputStream));
    }

    @Override
    public void writeSuggestUpdate(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        String uri = request.getRequestURI();

        Map data = new HashMap();

        data.put("assetsPath", "../assets/");
        data.put("date", new Date());
        data.put("requestUri", uri);
        data.put("address", AbstractNode.getState().getAddressPath());
        data.put("page", "Modify fragment: " + AbstractNode.getState().getIndex().getPredicate(request.getParameter("id")));
        data.put("id", request.getParameter("id"));
        data.put("updates", AbstractNode.getState().getPendingUpdates());
        data.put("numUpdates", AbstractNode.getState().getPendingUpdates().size());

        boolean search = request.getParameter("search") != null && !request.getParameter("search").equals("");
        List<Triple> triples = new ArrayList<>();
        HDT hdt = AbstractNode.getState().getDatasource(request.getParameter("id")).getHdt();

        if(!search) {
            IteratorTripleString iterator = hdt.search("", "", "");
            int count = 0;
            while(iterator.hasNext() && count < 20) {
                TripleString triple = iterator.next();
                triples.add(new Triple(triple.getSubject().toString(), triple.getPredicate().toString(), StringEscapeUtils.escapeHtml4(triple.getObject().toString())));
                count++;
            }
        } else {
            String s = request.getParameter("search");

            // Subject position
            IteratorTripleString iterator = hdt.search(s, "", "");

            while(iterator.hasNext()) {
                TripleString triple = iterator.next();
                triples.add(new Triple(triple.getSubject().toString(), triple.getPredicate().toString(), StringEscapeUtils.escapeHtml4(triple.getObject().toString())));
            }

            // predicate position
            iterator = hdt.search("", s, "");

            while(iterator.hasNext()) {
                TripleString triple = iterator.next();
                triples.add(new Triple(triple.getSubject().toString(), triple.getPredicate().toString(), StringEscapeUtils.escapeHtml4(triple.getObject().toString())));
            }

            // Object position
            iterator = hdt.search("", "", s);

            while(iterator.hasNext()) {
                TripleString triple = iterator.next();
                triples.add(new Triple(triple.getSubject().toString(), triple.getPredicate().toString(), StringEscapeUtils.escapeHtml4(triple.getObject().toString())));
            }
        }

        data.put("isSearch", search);
        data.put("triples", triples);

        modifyTemplate.process(data, new OutputStreamWriter(outputStream));
    }

    @Override
    public void writeSuggestedUpdate(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        String uri = request.getRequestURI();
        String id = request.getParameter("id");
        ITransaction t = AbstractNode.getState().getSuggestedTransaction(id);

        Map data = new HashMap();

        data.put("assetsPath", "../assets/");
        data.put("date", new Date());
        data.put("requestUri", uri);
        data.put("address", AbstractNode.getState().getAddressPath());
        data.put("page", "Update: " + request.getParameter("id"));
        data.put("id", request.getParameter("id"));
        data.put("updates", AbstractNode.getState().getPendingUpdates());
        data.put("numUpdates", AbstractNode.getState().getPendingUpdates().size());

        boolean search = request.getParameter("search") != null && !request.getParameter("search").equals("");
        List<TripleData> operations = new ArrayList<>();
        List<Operation> os = t.getOperations();

        if(!search) {
            for (Operation o : os) {
                String type = o.getType().toString();
                String subj = o.getTriple().getSubject();
                String pred = o.getTriple().getPredicate();
                String obj = o.getTriple().getObject();

                operations.add(new TripleData(type, subj, pred, obj));
            }
        } else {
            String s = request.getParameter("search");

            for (Operation o : os) {
                String type = o.getType().toString();
                String subj = o.getTriple().getSubject();
                String pred = o.getTriple().getPredicate();
                String obj = o.getTriple().getObject();

                if(s.equals(subj) || s.equals(pred) || s.equals(obj))
                    operations.add(new TripleData(type, subj, pred, obj));
            }
        }

        data.put("operations", operations);

        updateTemplate.process(data, new OutputStreamWriter(outputStream));
    }

    @Override
    public void writeQueryResults(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        if (error) {
            outputStream.println("There was an error. Please restart the client.");
            return;
        }
        String uri = request.getRequestURI().replace("api/sparql", "").replace("?", "");
        String sparql = request.getParameter("query");
        long timestamp = -1;
        String dateStr = "";
        if (request.getParameter("time") != null && !request.getParameter("time").equals(""))
            timestamp = Long.parseLong(request.getParameter("time"));
        if (request.getParameter("date") != null && !request.getParameter("date").equals("")) {
            dateStr = request.getParameter("date");
            DateTimeFormatter formatDateTime = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");
            LocalDateTime localDateTime = LocalDateTime.from(formatDateTime.parse(dateStr));
            Timestamp ts = Timestamp.valueOf(localDateTime);
            timestamp = ts.getTime();
        }

        Map data = new HashMap();

        data.put("assetsPath", "../assets/");
        data.put("date", new Date());
        data.put("requestUri", uri);
        data.put("address", AbstractNode.getState().getAddressPath());
        data.put("page", "Query result");
        data.put("query", sparql);
        data.put("timestamp", timestamp);
        data.put("dte", dateStr);
        data.put("updates", AbstractNode.getState().getPendingUpdates());
        data.put("numUpdates", AbstractNode.getState().getPendingUpdates().size());

        long startTime = System.currentTimeMillis();
        Query query = QueryFactory.create(sparql);
        final ColchainGraph graph;
        if (timestamp == -1)
            graph = new ColchainGraph();
        else
            graph = new ColchainGraph(timestamp);
        Model model = ModelFactory.createModelForGraph(graph);

        final QueryExecution executor = QueryExecutionFactory.create(query, model);
        final ResultSet rs = executor.execSelect();

        int cnt = 0;
        List<String> vars = new ArrayList<>();
        List<List<String>> values = new ArrayList<>();
        boolean first = true;
        while (rs.hasNext()) {
            QuerySolution sol = rs.next();
            cnt++;
            Iterator<String> vs = sol.varNames();
            List<String> vals = new ArrayList<>();
            while (vs.hasNext()) {
                String var = vs.next();
                if (first) {
                    vars.add(var);
                }
                vals.add(sol.get(var).toString());
            }
            values.add(vals);
            if (first) first = false;
        }

        System.out.println("Query took " + (System.currentTimeMillis() - startTime) + " ms. to execute");

        data.put("count", cnt);
        data.put("variables", vars);
        data.put("values", values);

        queryTemplate.process(data, new OutputStreamWriter(outputStream));
    }

    @Override
    public void writeCommunityDetails(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        String uri = request.getRequestURI();

        Map data = new HashMap();

        data.put("assetsPath", "../assets/");
        data.put("date", new Date());
        data.put("requestUri", uri);
        data.put("address", AbstractNode.getState().getAddressPath());
        data.put("updates", AbstractNode.getState().getPendingUpdates());
        data.put("numUpdates", AbstractNode.getState().getPendingUpdates().size());

        Community c = AbstractNode.getState().getCommunity(request.getParameter("id"));

        data.put("page", c.getName());
        data.put("community", c);

        Set<String> ids = c.getFragmentIds();
        List<FragmentMetadata> fragments = new ArrayList<>();
        for (String id : ids) {
            IDataSource datasource = AbstractNode.getState().getDatasource(id);
            String pred = AbstractNode.getState().getIndex().getPredicate(id);
            fragments.add(new FragmentMetadata(id, datasource.numTriples(), datasource.numSubjects(),
                    datasource.numPredicates(), datasource.numObjects(), pred));
        }

        data.put("fragments", fragments);

        communityTemplate.process(data, new OutputStreamWriter(outputStream));
    }

    @Override
    public void writeFragmentSearch(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        String uri = request.getRequestURI();
        String predicate = request.getParameter("predicate");

        Map data = new HashMap();

        data.put("assetsPath", "../assets/");
        data.put("date", new Date());
        data.put("requestUri", uri);
        data.put("address", AbstractNode.getState().getAddressPath());
        data.put("updates", AbstractNode.getState().getPendingUpdates());
        data.put("numUpdates", AbstractNode.getState().getPendingUpdates().size());
        data.put("page", "Search: " + predicate);

        List<String> ids = AbstractNode.getState().getIndex().getByPredicate(predicate);
        List<FragmentMetadata> fragments = new ArrayList<>();
        for (String id : ids) {
            try {
                IDataSource datasource = AbstractNode.getState().getDatasource(id);
                fragments.add(new FragmentMetadata(id, datasource.numTriples(), datasource.numSubjects(),
                        datasource.numPredicates(), datasource.numObjects(), predicate));
            } catch (NullPointerException e) {
                continue;
            }
        }



        data.put("fragments", fragments);
        data.put("numFragments", fragments.size());

        predicateTemplate.process(data, new OutputStreamWriter(outputStream));
    }

    @Override
    public void writeFragmentDetails(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        String uri = request.getRequestURI();

        Map data = new HashMap();

        data.put("assetsPath", "../assets/");
        data.put("date", new Date());
        data.put("requestUri", uri);
        data.put("address", AbstractNode.getState().getAddressPath());
        data.put("updates", AbstractNode.getState().getPendingUpdates());
        data.put("numUpdates", AbstractNode.getState().getPendingUpdates().size());

        String id = request.getParameter("id");
        KnowledgeChain chain = AbstractNode.getState().getChain(id);

        String predicate = AbstractNode.getState().getIndex().getPredicate(id);
        data.put("page", predicate);

        List<ChainEntryData> entries = new ArrayList<>();
        ChainEntry e = chain.top();

        while(!e.isFirst()) {
            ITransaction t = e.getTransaction();
            int additions = 0, deletions = 0;
            List<Operation> ops = t.getOperations();

            for(Operation op : ops) {
                if(op.getType() == Operation.OperationType.ADD)
                    additions++;
                else
                    deletions++;
            }

            entries.add(new ChainEntryData(t.getId(), t.getTimestamp(), t.getAuthor(), additions, deletions));
            e = e.previous();
        }

        data.put("chain", entries);
        data.put("fragmentId", id);

        fragmentTemplate.process(data, new OutputStreamWriter(outputStream));
    }

    @Override
    public void writeTransactionDetails(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        String uri = request.getRequestURI();

        Map data = new HashMap();

        data.put("assetsPath", "../assets/");
        data.put("date", new Date());
        data.put("requestUri", uri);
        data.put("address", AbstractNode.getState().getAddressPath());
        data.put("updates", AbstractNode.getState().getPendingUpdates());
        data.put("numUpdates", AbstractNode.getState().getPendingUpdates().size());
        data.put("page", "Transaction: " + request.getParameter("id"));
        data.put("id", request.getParameter("id"));
        data.put("fragment", request.getParameter("fragment"));

        List<TripleData> operations = new ArrayList<>();

        ChainEntry e = AbstractNode.getState().getChain(request.getParameter("fragment")).top();
        ITransaction t = null;
        while(t == null && !e.isFirst()) {
            if(e.getTransaction().getId().equals(request.getParameter("id"))) {
                t = e.getTransaction();
            }
            e = e.previous();
        }

        String author = "";
        String date = "";
        long timestamp = 0;
        if(t != null) {
            boolean search = request.getParameter("search") != null && !request.getParameter("search").equals("");

            if (!search) {
                for(Operation op : t.getOperations()) {
                    String type = op.getType().toString();
                    String subject = op.getTriple().getSubject();
                    String predicate = op.getTriple().getPredicate();
                    String object = op.getTriple().getObject();
                    operations.add(new TripleData(type, subject, predicate, object));
                }
            } else {
                String s = request.getParameter("search");
                for(Operation op : t.getOperations()) {
                    String type = op.getType().toString();
                    String subject = op.getTriple().getSubject();
                    String predicate = op.getTriple().getPredicate();
                    String object = op.getTriple().getObject();
                    if(subject.equals(s) || predicate.equals(s) || object.equals(s))
                        operations.add(new TripleData(type, subject, predicate, object));
                }
            }
            author = t.getAuthor();
            timestamp = t.getTimestamp();

            Date d = new Date();
            d.setTime(timestamp);
            date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm").format(d);
        }

        data.put("operations", operations);
        data.put("author", author);
        data.put("day", date);
        data.put("timestamp", timestamp);

        transactionTemplate.process(data, new OutputStreamWriter(outputStream));
    }

    public class FragmentMetadata {
        public FragmentMetadata(String id, int triples, int subjects, int predicates, int objects, String predicate) {
            this.id = id;
            this.triples = triples;
            this.subjects = subjects;
            this.predicates = predicates;
            this.objects = objects;
            this.predicate = predicate;
        }

        private String predicate;
        private String id;
        private int triples;
        private int subjects;
        private int predicates;
        private int objects;

        public String getId() {
            return id;
        }

        public String getTriples() {
            return Integer.toString(triples);
        }

        public String getSubjects() {
            return Integer.toString(subjects);
        }

        public String getPredicates() {
            return Integer.toString(predicates);
        }

        public String getObjects() {
            return Integer.toString(objects);
        }

        public String getPredicate() {
            return predicate;
        }
    }

    public class TripleData {
        private String type;
        private String subject;
        private String predicate;
        private String object;

        public TripleData(String type, String subject, String predicate, String object) {
            this.type = type;
            this.subject = subject;
            this.predicate = predicate;
            this.object = object;
        }

        public String getType() {
            return type;
        }

        public String getSubject() {
            return subject;
        }

        public String getPredicate() {
            return predicate;
        }

        public String getObject() {
            return object;
        }

        public boolean isAddition() {
            return type.equals("ADD");
        }
    }

    public class ChainEntryData {
        private String id;
        private String date;
        private String author;
        private int additions;
        private int deletions;

        public ChainEntryData(String id, long timestamp, String author, int additions, int deletions) {
            this.id = id;
            this.author = author;
            this.additions = additions;
            this.deletions = deletions;

            Date d = new Date();
            d.setTime(timestamp);
            date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm").format(d);
        }

        public String getId() {
            return id;
        }

        public String getDate() {
            return date;
        }

        public String getAuthor() {
            return author;
        }

        public String getAdditions() {
            return Integer.toString(additions);
        }

        public String getDeletions() {
            return Integer.toString(deletions);
        }
    }
}
