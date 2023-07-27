package org.colchain.colchain.servlet;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import jakarta.servlet.*;
import jakarta.servlet.http.Part;
import org.apache.commons.io.IOUtils;
import org.colchain.colchain.knowledgechain.impl.ChainEntry;
import org.colchain.colchain.util.ChainSerializer;
import org.colchain.colchain.util.TransactionSerializer;
import org.colchain.index.graph.IGraph;
import org.colchain.index.graph.impl.Graph;
import org.colchain.index.index.IIndex;
import org.colchain.index.ppbf.IBloomFilter;
import org.colchain.index.ppbf.impl.PrefixPartitionedBloomFilter;
import org.colchain.index.util.Triple;
import org.colchain.index.util.Tuple;
import org.colchain.colchain.community.Community;
import org.colchain.colchain.community.CommunityMember;
import org.colchain.colchain.knowledgechain.impl.KnowledgeChain;
import org.colchain.colchain.node.AbstractNode;
import org.colchain.colchain.transaction.ITransaction;
import org.colchain.colchain.transaction.Operation;
import org.colchain.colchain.transaction.impl.TransactionFactory;
import org.colchain.colchain.transaction.impl.TransactionImpl;
import org.colchain.colchain.util.CryptoUtils;
import org.colchain.colchain.util.HdtUtils;
import org.colchain.colchain.writer.IResponseWriter;
import org.colchain.colchain.writer.ResponseWriterFactory;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.linkeddatafragments.datasource.DataSourceFactory;
import org.linkeddatafragments.datasource.IDataSource;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.File;

import java.io.FileInputStream;
import java.io.IOException;

import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;

public class NodeApiServlet extends HttpServlet {
    private static final MultipartConfigElement MULTI_PART_CONFIG = new MultipartConfigElement("temp");
    private final IResponseWriter writer = ResponseWriterFactory.createWriter();
    private final CloseableHttpClient httpClient = HttpClients.createDefault();

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String contentType = request.getContentType();
        if (contentType != null && contentType.startsWith("multipart/")) {
            request.setAttribute(org.eclipse.jetty.server.Request.__MULTIPART_CONFIG_ELEMENT, MULTI_PART_CONFIG);
        } else {
            try {
                writer.writeInit(response.getOutputStream(), request);
            } catch (Exception e) {
                throw new ServletException(e);
            }
            return;
        }

        String contextPath = request.getContextPath();
        String requestURI = request.getRequestURI();

        String path = contextPath == null
                ? requestURI
                : requestURI.substring(contextPath.length());

        String endpoint = path.substring(path.lastIndexOf("/") + 1);
        String filename = "";

        if(endpoint.equals("upload")) {
            Part part = request.getPart("hdtfile");
            if (part == null || part.getSize() == 0) {
                try {
                    writer.writeInit(response.getOutputStream(), request);
                } catch (Exception e) {
                    throw new ServletException(e);
                }
                return;
            } else {
                filename = part.getSubmittedFileName();
                InputStream is = part.getInputStream();
                FileUtils.copyInputStreamToFile(is, new File(filename));
            }

            if(filename.equals("")) {
                response.getWriter().println("HDT file does not exist.");
                return;
            }
            File f = new File(filename);
            String community = IOUtils.toString(request.getPart("community").getInputStream(), Charset.defaultCharset());
            if (!f.exists()) {
                response.getWriter().println("HDT file does not exist.");
                return;
            }

            HdtUtils.upload(filename, community);
            System.out.println("Uploaded!");

            try {
                writer.writeLandingPage(response.getOutputStream(), request);
            } catch (Exception e) {
                throw new ServletException(e);
            }
        }
    }

    /**
     * @param request
     * @param response
     * @throws ServletException
     */
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (!WebInterfaceServlet.INIT) {
            try {
                writer.writeNotInitiated(response.getOutputStream(), request);
            } catch (Exception e) {
                throw new ServletException(e);
            }
            return;
        }

        String contextPath = request.getContextPath();
        String requestURI = request.getRequestURI();

        String path = contextPath == null
                ? requestURI
                : requestURI.substring(contextPath.length());

        String endpoint = path.substring(path.lastIndexOf("/") + 1);

        switch (endpoint) {
            case "upload":
                handleUpload(request, response);
                break;
            case "download":
                handleDownload(request, response);
                break;
            case "community":
                handleCommunity(request, response);
                break;
            case "sparql":
                handleSparql(request, response);
                break;
            case "update":
                handleUpdate(request, response);
                break;
            case "save":
                handleSave(request, response);
                break;
            case "fragment":
                handleFragment(request, response);
                break;
            case "date":
                handleDate(request, response);
                break;
            default:
                response.getWriter().println("Unknown request.");
        }
    }

    private void handleDate(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            writer.writeLandingPage(response.getOutputStream(), request);
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private void handleSave(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String filename = request.getParameter("filename");
        if(filename == null || filename.equals("")) return;

        AbstractNode.getState().saveState(filename);
        File file = new File(filename);
        if(!file.exists()){
            throw new ServletException("File doesn't exists on server.");
        }

        ServletContext ctx = getServletContext();
        InputStream fis = new FileInputStream(file);
        String mimeType = ctx.getMimeType(file.getAbsolutePath());
        response.setContentType(mimeType != null? mimeType:"application/cc-state");
        response.setContentLength((int) file.length());
        response.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");

        ServletOutputStream os = response.getOutputStream();
        byte[] bufferData = new byte[1024];
        int read = 0;
        while((read = fis.read(bufferData))!= -1){
            os.write(bufferData, 0, read);
        }
        os.flush();
        os.close();
        fis.close();

        /*try {
            writer.writeRedirect(response.getOutputStream(), request, "api/save");
        } catch (Exception e) {
            throw new ServletException(e);
        }*/
    }

    private void handleFragment(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String mode = request.getParameter("mode");
        if(mode.equals("details")) {
            try {
                writer.writeFragmentDetails(response.getOutputStream(), request);
            } catch (Exception e) {
                throw new ServletException(e);
            }
            return;
        } else if(mode.equals("operations")) {
            try {
                writer.writeTransactionDetails(response.getOutputStream(), request);
            } catch (Exception e) {
                throw new ServletException(e);
            }
            return;
        }

        try {
            writer.writeRedirect(response.getOutputStream(), request, "/api/fragment");
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private void handleUpdate(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String mode = request.getParameter("mode");
        if(mode.equals("create")) {
            try {
                writer.writeSuggestUpdate(response.getOutputStream(), request);
            } catch (Exception e) {
                throw new ServletException(e);
            }
            return;
        } else if(mode.equals("suggest")) {
            String id = request.getParameter("id");
            String content = URLDecoder.decode(request.getParameter("content"), "UTF-8");
            ITransaction t = TransactionFactory.getTransaction(getOperations(content), id, AbstractNode.getState().getId());
            byte[] signature = CryptoUtils.createSignature(AbstractNode.getState().getPrivateKey(), t);
            AbstractNode.getState().suggestTransaction(t, signature);

            Community c = AbstractNode.getState().getCommunityByFragmentId(t.getFragmentId());
            Set<CommunityMember> parts = c.getParticipants();
            for(CommunityMember p : parts) {
                if(p.getAddress().equals(AbstractNode.getState().getAddress())) continue;
                p.suggestTransaction(t, signature);
            }
        } else if(mode.equals("new")) {
            Gson gson = new Gson();
            Type t1 = new TypeToken<TransactionImpl>() {}.getType();
            Type t2 = new TypeToken<byte[]>() {}.getType();

            ITransaction t = gson.fromJson(request.getParameter("trans"), t1);
            byte[] signature = gson.fromJson(request.getParameter("sign"), t2);
            AbstractNode.getState().suggestTransaction(t, signature);
            return;
        } else if(mode.equals("accept")) {
            String tid = request.getParameter("trans");
            String nid = request.getParameter("node");

            AbstractNode.getState().acceptTransaction(tid, nid);
            return;
        } else if(mode.equals("acc")) {
            String tid = request.getParameter("id");

            AbstractNode.getState().accept(AbstractNode.getState().getSuggestedTransaction(tid));
        } else if(mode.equals("view")) {
            try {
                writer.writeSuggestedUpdate(response.getOutputStream(), request);
            } catch (Exception e) {
                throw new ServletException(e);
            }
            return;
        } else if(mode.equals("index")) {
            downloadIndex(request.getParameter("address"), request.getParameter("id"));
            return;
        } else if(mode.equals("fragment")) {
            AbstractNode.getState().getDatasource(request.getParameter("id")).reload();
            return;
        }

        try {
            writer.writeRedirect(response.getOutputStream(), request, "/api/update");
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private List<Operation> getOperations(String content) {
        List<Operation> ops = new ArrayList<>();

        String[] strs = content.split("\n");
        for(String s : strs) {
            if(!s.startsWith("+") && !s.startsWith("-")) continue;
            String st = s;
            Operation.OperationType type;
            if(s.startsWith("+")) {
                type = Operation.OperationType.ADD;
                st = st.replace("+", "").replace(" ", "").replace("(", "").replace(")", "");
            } else {
                type = Operation.OperationType.DEL;
                st = st.replace("-", "").replace(" ", "").replace("(", "").replace(")", "");
            }

            String[] uris = st.split(",");
            Triple triple = new Triple(uris[0], uris[1], uris[2]);
            ops.add(new Operation(type, triple));
        }

        return ops;
    }

    private void handleSparql(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            writer.writeQueryResults(response.getOutputStream(), request);
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private void handleUpload(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String path = request.getParameter("path");
        String community = request.getParameter("community");
        File f = new File(path);
        if (!f.exists()) {
            response.getWriter().println("HDT file does not exist.");
            return;
        }

        HdtUtils.upload(path, community);
        System.out.println("Uploaded!");

        try {
            writer.writeLandingPage(response.getOutputStream(), request);
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private void handleDownload(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String type = request.getParameter("type");
        String filename = request.getParameter("file");
        if (type.equals("fragment")) {
            String fileSource = AbstractNode.getState().getDatastore() + "hdt/" + filename;
            File file = new File(fileSource);
            FileUtils.copyFile(file, response.getOutputStream());
            response.getOutputStream().flush();
        } else if(type.equals("chain")) {
            Gson gson = new Gson();
            KnowledgeChain chain = AbstractNode.getState().getChain(filename);
            response.getOutputStream().println(gson.toJson(chain));
        } else if(type.equals("index")) {
            String fileSource = AbstractNode.getState().getDatastore() + "index/" + filename;
            File file = new File(fileSource);
            FileUtils.copyFile(file, response.getOutputStream());
            response.getOutputStream().flush();
        } else if(type.equals("graph")) {
            Gson gson = new Gson();
            IGraph graph = AbstractNode.getState().getIndex().getGraph(filename);
            response.getOutputStream().println(gson.toJson(graph));
        } else if (type.equals("updates")) {
            Gson gson = new GsonBuilder().registerTypeAdapter(ITransaction.class, new TransactionSerializer()).create();

            Map<String, Tuple<ITransaction, Set<String>>> map = new HashMap<>();
            Map<String, Tuple<ITransaction, Set<String>>> updates = AbstractNode.getState().getTransactions();
            List<ITransaction> pending = AbstractNode.getState().getPendingUpdates();
            for(ITransaction t : pending) {
                if(t.getFragmentId().equals(filename)) {
                    map.put(t.getId(), updates.get(t.getId()));
                }
            }

            response.getOutputStream().println(gson.toJson(map));
        } else {
            response.getWriter().println("Error");
        }
    }

    private void handleCommunity(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String mode = request.getParameter("mode");
        if (mode.equals("create")) {
            Community c = new Community(request.getParameter("name"));
            AbstractNode.getState().addCommunity(c);
        } else if (mode.equals("leave")) {
            String id = request.getParameter("id");
            Community c = AbstractNode.getState().getCommunity(id);

            Set<CommunityMember> parts = c.getParticipants();
            for (CommunityMember m : parts) {
                if(m.getAddress().equals(AbstractNode.getState().getAddress())) continue;
                String a = m.getAddress() + (m.getAddress().endsWith("/") ? "" : "/") + "api/community?mode=removeMem&id="
                        + AbstractNode.getState().getId() + "&address=" + AbstractNode.getState().getAddress() + "&community=" + id;
                performAction(a);
            }

            Set<CommunityMember> obs = c.getObservers();
            for (CommunityMember m : obs) {
                if(m.getAddress().equals(AbstractNode.getState().getAddress())) continue;
                String a = m.getAddress() + (m.getAddress().endsWith("/") ? "" : "/") + "api/community?mode=removeMem&id="
                        + AbstractNode.getState().getId() + "&address=" + AbstractNode.getState().getAddress() + "&community=" + id;
                performAction(a);
            }

            AbstractNode.getState().leaveCommunity(id);
        } else if (mode.equals("search")) {
            Set<Tuple<String, Tuple<String, String>>> communities = findCommunities(request.getParameter("address"));
            try {
                writer.writeSearch(response.getOutputStream(), request, communities);
            } catch (Exception e) {
                throw new ServletException(e);
            }
            return;
        } else if (mode.equals("list")) {
            String str = "";
            List<Community> cs = AbstractNode.getState().getCommunities();
            for (Community c : cs) {
                str += c.getName() + ";" + c.getId() + "\n";
            }
            str = replaceLast(str, "\n", "");
            response.getWriter().println(str);
            return;
        } else if (mode.equals("meta")) {
            String id = request.getParameter("id");
            Community c = AbstractNode.getState().getCommunity(id);

            Gson gson = new Gson();
            String json = gson.toJson(c);
            response.getWriter().println(json);
            return;
        } else if (mode.equals("participate")) {
            String id = request.getParameter("id");
            String address = request.getParameter("address");
            address = address + (address.endsWith("/") ? "" : "/");

            Community c = fetchCommunity(address + "api/community?mode=meta&id=" + id);
            c.setMemberType(Community.MemberType.PARTICIPANT);

            Set<CommunityMember> parts = c.getParticipants();
            for (CommunityMember m : parts) {
                if(m.getAddress().equals(AbstractNode.getState().getAddress())) continue;
                String a = m.getAddress() + (m.getAddress().endsWith("/") ? "" : "/") + "api/community?mode=addPart&id="
                        + AbstractNode.getState().getId() + "&address=" + AbstractNode.getState().getAddress() + "&community=" + id;
                performAction(a);
            }

            Set<CommunityMember> obs = c.getObservers();
            for (CommunityMember m : obs) {
                if(m.getAddress().equals(AbstractNode.getState().getAddress())) continue;
                String a = m.getAddress() + (m.getAddress().endsWith("/") ? "" : "/") + "api/community?mode=addPart&id="
                        + AbstractNode.getState().getId() + "&address=" + AbstractNode.getState().getAddress() + "&community=" + id;
                performAction(a);
            }

            Set<String> fids = c.getFragmentIds();
            for(String fid : fids) {
                downloadFragment(address, fid);
                downloadIndex(address, fid);
            }

            c.addParticipant(new CommunityMember(AbstractNode.getState().getId(), AbstractNode.getState().getAddress()));

            AbstractNode.getState().addCommunity(c);
        } else if (mode.equals("observe")) {
            String id = request.getParameter("id");
            String address = request.getParameter("address");
            address = address + (address.endsWith("/") ? "" : "/");

            Community c = fetchCommunity(address + "api/community?mode=meta&id=" + id);
            Set<CommunityMember> members = c.getParticipants();
            for(CommunityMember cm : members) {
                if(cm.getAddress().contains("localhost")) {
                    CommunityMember mem = new CommunityMember(cm.getId(), address);
                    c.getParticipants().remove(cm);
                    c.getParticipants().add(mem);
                }
            }

            c.setMemberType(Community.MemberType.OBSERVER);

            Set<CommunityMember> parts = c.getParticipants();
            for (CommunityMember m : parts) {
                if(m.getAddress().equals(AbstractNode.getState().getAddress())) continue;
                String a = m.getAddress() + (m.getAddress().endsWith("/") ? "" : "/") + "api/community?mode=addObs&id="
                        + AbstractNode.getState().getId() + "&address=" + AbstractNode.getState().getAddress() + "&community=" + id;
                performAction(a);
            }

            Set<CommunityMember> obs = c.getObservers();
            for (CommunityMember m : obs) {
                if(m.getAddress().equals(AbstractNode.getState().getAddress())) continue;
                String a = m.getAddress() + (m.getAddress().endsWith("/") ? "" : "/") + "api/community?mode=addObs&id="
                        + AbstractNode.getState().getId() + "&address=" + AbstractNode.getState().getAddress() + "&community=" + id;
                performAction(a);
            }

            Set<String> fids = c.getFragmentIds();
            for(String fid : fids) {
                downloadIndex(address, fid);
            }

            c.addObserver(new CommunityMember(AbstractNode.getState().getId(), AbstractNode.getState().getAddress()));

            AbstractNode.getState().addCommunity(c);
        } else if (mode.equals("addPart")) {
            String id = request.getParameter("id");
            String address = request.getParameter("address");
            String community = request.getParameter("community");

            AbstractNode.getState().addParticipant(new CommunityMember(id, address), community);
            return;
        } else if (mode.equals("addObs")) {
            String id = request.getParameter("id");
            String address = request.getParameter("address");
            String community = request.getParameter("community");

            AbstractNode.getState().addObserver(new CommunityMember(id, address), community);
            return;
        } else if (mode.equals("removeMem")) {
            String id = request.getParameter("id");
            String address = request.getParameter("address");
            String community = request.getParameter("community");

            AbstractNode.getState().removeMember(new CommunityMember(id, address), community);
            return;
        } else if (mode.equals("newFragment")) {
            String id = request.getParameter("id");
            String address = request.getParameter("address");
            address = address + (address.endsWith("/") ? "" : "/");
            String community = request.getParameter("community");

            Community c = AbstractNode.getState().getCommunity(community);
            c.addFragment(id);
            if(c.getMemberType() == Community.MemberType.PARTICIPANT) {
                downloadFragment(address, id);
            }
            downloadIndex(address, id);
            return;
        } else if(mode.equals("details")) {
            try {
                writer.writeCommunityDetails(response.getOutputStream(), request);
            } catch (Exception e) {
                throw new ServletException(e);
            }
            return;
        } else if (mode.equals("predicate")) {
            try {
                writer.writeFragmentSearch(response.getOutputStream(), request);
            } catch (Exception e) {
                throw new ServletException(e);
            }
            return;
        }

        try {
            writer.writeLandingPage(response.getOutputStream(), request);
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private Set<Tuple<String, Tuple<String, String>>> findCommunities(String address) {
        Set<Tuple<String, Tuple<String, String>>> ret = new HashSet<>();

        if (address == null || address.equals("")) {
            List<Community> communities = AbstractNode.getState().getCommunities();
            for (Community c : communities) {
                Set<CommunityMember> parts = c.getParticipants();
                for (CommunityMember mem : parts) {
                    if (mem.getAddress().equals(AbstractNode.getState().getAddress()))
                        continue;
                    String addr = mem.getAddress();
                    String uri = addr + (addr.endsWith("/") ? "" : "/") + "/api/community?mode=list";
                    List<Tuple<String, String>> lst = makeRequest(uri);

                    for (Tuple<String, String> cc : lst) {
                        ret.add(new Tuple<>(addr, cc));
                    }
                }

                Set<CommunityMember> obs = c.getObservers();
                for (CommunityMember mem : obs) {
                    if (mem.getAddress().equals(AbstractNode.getState().getAddress()))
                        continue;
                    String addr = mem.getAddress();
                    String uri = addr + (addr.endsWith("/") ? "" : "/") + "/api/community?mode=list";
                    List<Tuple<String, String>> lst = makeRequest(uri);

                    for (Tuple<String, String> cc : lst) {
                        ret.add(new Tuple<>(addr, cc));
                    }
                }
            }
        } else {
            String uri = address + (address.endsWith("/") ? "" : "/") + "/api/community?mode=list";
            List<Tuple<String, String>> lst = makeRequest(uri);

            for (Tuple<String, String> c : lst) {
                ret.add(new Tuple<>(address, c));
            }
        }

        return ret;
    }

    private void downloadFragment(String address, String id) {
        String url = address + "api/download?type=fragment&file=" + id + ".hdt";

        String filename = AbstractNode.getState().getDatastore() + "hdt/" + id + ".hdt";
        Content content = null;
        try {
            content = Request.Get(url).execute().returnContent();
            InputStream stream = content.asStream();
            Files.copy(stream, Paths.get(".", filename), StandardCopyOption.REPLACE_EXISTING);
            stream.close();
        } catch (IOException e) {
            return;
        }

        filename = filename + ".index.v1-1";
        url = url + ".index.v1-1";
        try {
            content = Request.Get(url).execute().returnContent();
            InputStream stream = content.asStream();
            Files.copy(stream, Paths.get(".", filename), StandardCopyOption.REPLACE_EXISTING);
            stream.close();
        } catch (IOException e) {
            return;
        }

        url = address + "api/download?type=chain&file=" + id;
        try {
            content = Request.Get(url).execute().returnContent();
            String json = content.asString();
            Type type = new TypeToken<KnowledgeChain>() {
            }.getType();
            Gson gson = new GsonBuilder().registerTypeAdapter(ChainEntry.class, new ChainSerializer()).create();

            KnowledgeChain chain = gson.fromJson(json, type);
            IDataSource ds = DataSourceFactory.createLocal(id, AbstractNode.getState().getDatastore() + "hdt/" + id + ".hdt");
            chain.setDatasource(ds);

            AbstractNode.getState().addChain(id, chain);
        } catch (IOException e) {
            return;
        }

        url = address + "api/download?type=updates&file=" + id;
        try {
            content = Request.Get(url).execute().returnContent();
            String json = content.asString();
            Type type = new TypeToken<Map<String, Tuple<ITransaction, Set<String>>>>() {
            }.getType();
            Gson gson = new GsonBuilder().registerTypeAdapter(ITransaction.class, new TransactionSerializer()).create();

            Map<String, Tuple<ITransaction, Set<String>>> map = gson.fromJson(json, type);
            AbstractNode.getState().addPending(map);
        } catch (IOException e) {
            return;
        }
    }

    private void downloadIndex(String address, String id) {
        String url = address + "api/download?type=index&file=" + id + ".hdt.ppbf";

        String filename = AbstractNode.getState().getDatastore() + "index/" + id + ".hdt.ppbf";
        File f = new File(filename);
        if(f.exists()) f.delete();
        Content content = null;
        try {
            content = Request.Get(url).execute().returnContent();
            InputStream stream = content.asStream();
            Files.copy(stream, Paths.get(".", filename), StandardCopyOption.REPLACE_EXISTING);
            stream.close();
        } catch (IOException e) {
            return;
        }

        filename = filename + "p";
        f = new File(filename);
        if(f.exists()) f.delete();
        url = url + "p";
        try {
            content = Request.Get(url).execute().returnContent();
            InputStream stream = content.asStream();
            Files.copy(stream, Paths.get(".", filename), StandardCopyOption.REPLACE_EXISTING);
            stream.close();
        } catch (IOException e) {
            return;
        }

        url = address + "api/download?type=graph&file=" + id;
        IGraph graph;
        try {
            content = Request.Get(url).execute().returnContent();
            Type type = new TypeToken<Graph>() {}.getType();
            Gson gson = new Gson();
            graph = gson.fromJson(content.asString(), type);
        } catch (IOException e) {
            return;
        }

        IBloomFilter<String> filter = PrefixPartitionedBloomFilter.create(AbstractNode.getState().getDatastore() + "index/" + id + ".hdt.ppbf");

        IIndex index = AbstractNode.getState().getIndex();
        if(index.hasFragment(id)) {
            index.updateIndex(id, filter);
        } else {
            index.addFragment(graph, filter);
        }
    }

    private Community fetchCommunity(String address) {
        Community c;

        HttpGet request = new HttpGet(address);
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                Type type = new TypeToken<Community>() {
                }.getType();
                String result = EntityUtils.toString(entity);
                System.out.println(result);
                Gson gson = new Gson();
                c = gson.fromJson(result, type);
            } else {
                return null;
            }

        } catch (IOException e) {
            return null;
        }

        return c;
    }

    private void performAction(String address) {
        HttpGet request = new HttpGet(address);
        try {
            httpClient.execute(request);
        } catch (IOException e) {
        }

    }

    private List<Tuple<String, String>> makeRequest(String address) {
        List<Tuple<String, String>> ret = new ArrayList<>();

        HttpGet request = new HttpGet(address);
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                String result = EntityUtils.toString(entity);
                String[] rs = result.split("\n");
                for (String r : rs) {
                    if (r.equals("")) continue;
                    String[] ws = r.split(";");
                    if (AbstractNode.getState().hasCommunity(ws[1]))
                        continue;
                    ret.add(new Tuple<>(ws[0], ws[1]));
                }
            }

        } catch (IOException e) {
            return ret;
        }

        return ret;
    }

    private String replaceLast(String string, String toReplace, String replacement) {
        int pos = string.lastIndexOf(toReplace);
        if (pos > -1) {
            return string.substring(0, pos)
                    + replacement
                    + string.substring(pos + toReplace.length());
        } else {
            return string;
        }
    }
}
