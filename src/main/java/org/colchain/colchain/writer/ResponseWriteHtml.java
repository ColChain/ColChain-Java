package org.colchain.colchain.writer;

import com.google.gson.Gson;
import org.colchain.index.graph.IGraph;
import org.colchain.index.util.Tuple;
import org.colchain.colchain.community.Community;
import org.colchain.colchain.node.AbstractNode;
import org.colchain.colchain.sparql.graph.ColchainGraph;
import org.colchain.colchain.transaction.ITransaction;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ResponseWriteHtml implements IResponseWriter {
    @Override
    public void writeNotInitiated(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
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
        String uri = request.getRequestURI();
        if (!uri.endsWith("/"))
            uri += "/";

        String line = "<!doctype html>\n" +
                "<html>\n" +
                "<head>\n" +
                "<meta charset=\"utf-8\">\n" +
                "<title>Client</title>\n" +
                "</head>\n" +
                "<body>\n";

        line += "  <form action=\"api/save\">\n" +
                "    <input type=\"text\" id=\"filename\" name=\"filename\"> \n" +
                "    <input type=\"submit\" value=\"Save state\">\n" +
                "  </form>\n";

        line += "  <h1>Experiments</h1>\n" +
                "  <form action=\"experiments\">\n" +
                "    Performance Experiments:\n" +
                "    <input type=\"hidden\" id=\"mode\" name=\"mode\" value=\"performance\"> \n" +
                "    <label for=\"queries\">Query Directory:</label> \n" +
                "    <input type=\"text\" id=\"queries\" name=\"queries\"> \n" +
                "    <label for=\"out\">Output Directory:</label> \n" +
                "    <input type=\"text\" id=\"out\" name=\"out\"> \n" +
                "    <label for=\"reps\">Replications:</label> \n" +
                "    <input type=\"number\" id=\"reps\" name=\"reps\"> \n" +
                "    <input type=\"submit\" value=\"Run\">\n" +
                "  </form>\n" +
                "  <form action=\"experiments\">\n" +
                "    Versioning Experiments:\n" +
                "    <input type=\"hidden\" id=\"mode\" name=\"mode\" value=\"versioning\"> \n" +
                "    <label for=\"queries\">Query Directory:</label> \n" +
                "    <input type=\"text\" id=\"queries\" name=\"queries\"> \n" +
                "    <label for=\"out\">Output Directory:</label> \n" +
                "    <input type=\"text\" id=\"out\" name=\"out\"> \n" +
                "    <label for=\"reps\">Chain length:</label> \n" +
                "    <input type=\"number\" id=\"length\" name=\"length\"> \n" +
                "    <input type=\"submit\" value=\"Run\">\n" +
                "  </form>\n" +
                "  <form action=\"experiments\">\n" +
                "    Updates Experiments:\n" +
                "    <input type=\"hidden\" id=\"mode\" name=\"mode\" value=\"updates\"> \n" +
                "    <label for=\"queries\">Update Directory:</label> \n" +
                "    <input type=\"text\" id=\"updates\" name=\"updates\"> \n" +
                "    <label for=\"out\">Output Directory:</label> \n" +
                "    <input type=\"text\" id=\"out\" name=\"out\"> \n" +
                "    <label for=\"reps\">Chain length:</label> \n" +
                "    <input type=\"number\" id=\"length\" name=\"length\"> \n" +
                "    <input type=\"submit\" value=\"Run\">\n" +
                "  </form>\n";

        line += "  <h1>SPARQL Query</h1>\n" +
                "  <form action=\"api/sparql\" id=\"queryform\">\n" +
                "    <textarea name=\"query\" form=\"queryform\" cols=\"100\" rows=\"10\"></textarea><br/>\n" +
                "    <label for=\"time\">Timestamp:</label> \n" +
                "    <input type=\"number\" id=\"time\" name=\"time\"> \n" +
                "    <input type=\"submit\" value=\"Issue Query\">\n" +
                "  </form>\n";

        line += "  <h1>Communities</h1>\n" +
                "  Current communities:\n" +
                "  <ul>\n";

        List<Community> comms = AbstractNode.getState().getCommunities();
        for (Community c : comms) {
            line += "    <li>" + c.getName() + ": " + c.getMemberType().toString() + " (id: " + c.getId() + ", fragments: "
                    + c.getFragmentIds().size() + ", participants: "
                    + c.getParticipants().size() + ", observers: "
                    + c.getObservers().size() + ")\n" +
                    "  <form action=\"" + uri + "api/community\" method=\"get\">\n" +
                    "    <input type=\"hidden\" id=\"mode\" name=\"mode\" value=\"leave\">" +
                    "    <input type=\"hidden\" id=\"id\" name=\"id\" value=\"" + c.getId() + "\"> \n" +
                    "    <input type=\"submit\" value=\"Leave\">\n" +
                    "  </form>\n" +
                    "</li>\n";
        }

        line += "  </ul>\n" +
                "  <form action=\"" + uri + "api/community\" method=\"get\">\n" +
                "    <input type=\"hidden\" id=\"mode\" name=\"mode\" value=\"create\">" +
                "    <label for=\"name\">Community name:</label> \n" +
                "    <input type=\"text\" id=\"name\" name=\"name\"> \n" +
                "    <input type=\"submit\" value=\"Create\">\n" +
                "  </form>\n" +
                "  <form action=\"" + uri + "api/community\" method=\"get\">\n" +
                "    <input type=\"hidden\" id=\"mode\" name=\"mode\" value=\"search\">" +
                "    <label for=\"name\">Address (leave blank to search from neighbors):</label> \n" +
                "    <input type=\"text\" id=\"address\" name=\"address\"> \n" +
                "    <input type=\"submit\" value=\"Search for new Community\">\n" +
                "  </form>\n";

        line += "  <h1>Updates</h1>\n" +
                "  Pending updates:\n";

        List<ITransaction> ts = AbstractNode.getState().getPendingUpdates();
        if (ts.size() > 0) {
            line += "  <ul>\n";
        }

        for (ITransaction t : ts) {
            line += "<li>\n" +
                    t.getId() + " (FRAGMENT: " + t.getFragmentId() + ", AUTHOR: " + t.getAuthor() + ")" +
                    "  <form action=\"" + uri + "api/update\" method=\"get\">\n" +
                    "    <input type=\"hidden\" id=\"mode\" name=\"mode\" value=\"view\">" +
                    "    <input type=\"hidden\" id=\"id\" name=\"id\" value=\"" + t.getId() + "\">" +
                    "    <input type=\"submit\" value=\"View\">\n" +
                    "  </form>\n" +
                    "</li>\n";
        }

        if (ts.size() > 0) {
            line += "  </ul>\n";
        }


        line += "  <h1>Fragments</h1>\n" +
                "  Currently available fragments:\n";

        if (comms.size() > 0) {
            line += "  <ul>\n";
        }

        Set<String> fragments = AbstractNode.getState().getDatasourceIds();
        uri += "ldf/";
        for (String s : fragments) {
            Community c = null;
            for (Community com : comms) {
                if (com.isIn(s)) {
                    c = com;
                    break;
                }
            }

            line += "    <li><a href=\"" + uri + s + "\">" + s + "</a> (Community: " + ((c == null) ? "" : c.getName()) + ")" +
                    "  <form action=\"" + uri.replace("ldf/", "api/update") + "\" method=\"get\">\n" +
                    "    <input type=\"hidden\" id=\"mode\" name=\"mode\" value=\"create\">" +
                    "    <input type=\"hidden\" id=\"id\" name=\"id\" value=\"" + s + "\">" +
                    "    <input type=\"submit\" value=\"Suggest update\">\n" +
                    "  </form>\n" +
                    "    </li>\n";
        }

        if (comms.size() > 0) {

            line += "  </ul> " +
                    "  <form action=\"" + uri.replace("ldf/", "api/upload") + "\" method=\"get\"> " +
                    "    <label for=\"path\">HDT file path:</label> \n" +
                    "    <input type=\"text\" id=\"path\" name=\"path\"> \n" +
                    "    <label for=\"community\">Select community:</label> \n" +
                    "    <select id=\"community\" name=\"community\">\n";

            for (Community c : comms) {
                if (c.getMemberType() == Community.MemberType.PARTICIPANT)
                    line += "      <option value=\"" + c.getId() + "\">" + c.getName() + "</option>\n";
            }

            line += "    </select>\n" +
                    "    <input type=\"submit\" value=\"Upload\">\n" +
                    "  </form><br>\n";
        }

        line += "  <h1>Index</h1>\n" +
                "  Current fragments in the index:\n" +
                "  <ul>\n";

        Set<IGraph> graphs = AbstractNode.getState().getIndex().getGraphs();
        for (IGraph g : graphs) {
            String id = g.getId();
            String community = g.getCommunity();

            Community c = null;
            for (Community com : comms) {
                if (com.getId().equals(community)) {
                    c = com;
                    break;
                }
            }

            line += "    <li>" + id + " (Community: " + ((c == null) ? "" : c.getName()) + ")</li>\n";
        }

        line += "  </ul> \n";
        line += "</body>\n" +
                "</html>";


        outputStream.println(line);
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
        System.out.println(uri);
        String line = "<!doctype html>\n" +
                "<html>\n" +
                "<head>\n" +
                "<meta charset=\"utf-8\">\n" +
                "<title>Client</title>\n" +
                "</head>\n" +
                "<body>\n" +
                "  <form action=\"" + uri + "\">\n" +
                "    <input type=\"submit\" value=\"Back\">\n" +
                "  </form>\n";

        if (comms.size() == 0) {
            line += "No communities found!\n";
        } else {
            line += "  <ul>\n";
            for (Tuple<String, Tuple<String, String>> tpl : comms) {
                line += "<li>" + tpl.getSecond().getFirst() + " (ID: " + tpl.getSecond().getSecond() + ")\n" +
                        "  <form action=\"" + uri + "/api/community\" method=\"get\">\n" +
                        "    <input type=\"hidden\" id=\"mode\" name=\"mode\" value=\"participate\">" +
                        "    <input type=\"hidden\" id=\"id\" name=\"id\" value=\"" + tpl.getSecond().getSecond() + "\">" +
                        "    <input type=\"hidden\" id=\"address\" name=\"address\" value=\"" + tpl.getFirst() + (tpl.getFirst().endsWith("/") ? "" : "/") + "\">" +
                        "    <input type=\"submit\" value=\"Participate\">\n" +
                        "  </form>\n" +
                        "  <form action=\"" + uri + "/api/community\" method=\"get\">\n" +
                        "    <input type=\"hidden\" id=\"mode\" name=\"mode\" value=\"observe\">" +
                        "    <input type=\"hidden\" id=\"id\" name=\"id\" value=\"" + tpl.getSecond().getSecond() + "\">" +
                        "    <input type=\"hidden\" id=\"address\" name=\"address\" value=\"" + tpl.getFirst() + (tpl.getFirst().endsWith("/") ? "" : "/") + "\">" +
                        "    <input type=\"submit\" value=\"Observe\">\n" +
                        "  </form>\n" +
                        "</li>";
            }
            line += "  </ul>\n";
        }

        line += "</body>";

        outputStream.println(line);
    }

    @Override
    public void writeInit(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        String uri = request.getRequestURI();
        String line = "<!doctype html>\n" +
                "<html>\n" +
                "<head>\n" +
                "<meta charset=\"utf-8\">\n" +
                "<title>Client</title>\n" +
                "</head>\n" +
                "<body>\n" +
                "  <form action=\"" + uri + "\">\n" +
                "    <input type=\"hidden\" id=\"mode\" name=\"mode\" value=\"config\">" +
                "    <label for=\"name\">Config file:</label> \n" +
                "    <input type=\"text\" id=\"config\" name=\"config\"> \n" +
                "    <input type=\"submit\" value=\"Initiate\">\n" +
                "  </form>\n" +
                "  <form action=\"" + uri + "\">\n" +
                "    <input type=\"hidden\" id=\"mode\" name=\"mode\" value=\"read\">" +
                "    <label for=\"name\">State file:</label> \n" +
                "    <input type=\"text\" id=\"filename\" name=\"filename\"> \n" +
                "    <input type=\"submit\" value=\"Read\">\n" +
                "  </form>\n" +
                "  <form action=\"" + uri + "experiments\">\n" +
                "    <input type=\"hidden\" id=\"mode\" name=\"mode\" value=\"setup\">" +
                "    <label for=\"name\"> Config file:</label> \n" +
                "    <input type=\"text\" id=\"config\" name=\"config\"> \n" +
                "    <label for=\"name\"> File directory:</label> \n" +
                "    <input type=\"text\" id=\"dir\" name=\"dir\"> \n" +
                "    <label for=\"name\"> No. Nodes:</label> \n" +
                "    <input type=\"number\" id=\"nodes\" name=\"nodes\"> \n" +
                "    <label for=\"name\"> Replications:</label> \n" +
                "    <input type=\"number\" id=\"rep\" name=\"rep\"> \n" +
                "    <input type=\"submit\" value=\"Setup Experiments\">\n" +
                "  </form>\n" +
                "  <form action=\"" + uri + "experiments\">\n" +
                "    <input type=\"hidden\" id=\"mode\" name=\"mode\" value=\"start\">" +
                "    <label for=\"name\"> Config file:</label> \n" +
                "    <input type=\"text\" id=\"config\" name=\"config\"> \n" +
                "    <label for=\"name\"> File directory:</label> \n" +
                "    <input type=\"text\" id=\"dir\" name=\"dir\"> \n" +
                "    <label for=\"name\"> Setup directory:</label> \n" +
                "    <input type=\"text\" id=\"setup\" name=\"setup\"> \n" +
                "    <label for=\"name\"> Node ID:</label> \n" +
                "    <input type=\"number\" id=\"id\" name=\"id\"> \n" +
                "    <label for=\"name\"> No. Nodes:</label> \n" +
                "    <input type=\"number\" id=\"nodes\" name=\"nodes\"> \n" +
                "    <label for=\"name\"> Chain Length:</label> \n" +
                "    <input type=\"number\" id=\"chain\" name=\"chain\"> \n" +
                "    <input type=\"submit\" value=\"Start\">\n" +
                "  </form>\n" +
                "  <form action=\"" + uri + "experiments\">\n" +
                "    <input type=\"hidden\" id=\"mode\" name=\"mode\" value=\"startUpdates\">" +
                "    <label for=\"name\"> Config file:</label> \n" +
                "    <input type=\"text\" id=\"config\" name=\"config\"> \n" +
                "    <label for=\"name\"> Data Directory:</label> \n" +
                "    <input type=\"text\" id=\"data\" name=\"data\"> \n" +
                "    <input type=\"submit\" value=\"Start\">\n" +
                "  </form>\n" +
                "</body>";

        outputStream.println(line);
    }

    @Override
    public void writeSuggestUpdate(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        String uri = request.getRequestURI();
        String id = request.getParameter("id");

        String line = "<!doctype html>\n" +
                "<html>\n" +
                "<head>\n" +
                "<meta charset=\"utf-8\">\n" +
                "<title>Client</title>\n" +
                "</head>\n" +
                "<body>\n" +
                "  <form action=\"" + uri.replace("api/update", "") + "\">\n" +
                "    <input type=\"submit\" value=\"Back\">\n" +
                "  </form><br/>\n" +
                "  <form action=\"" + uri + "\" id=\"udpateform\">\n" +
                "    Operations (SYNTAX: [+|-] (subj, pred, obj). One line for each operation):<br/>\n" +
                "    <textarea name=\"content\" form=\"udpateform\" cols=\"100\" rows=\"30\"></textarea><br/>\n" +
                "    <input type=\"hidden\" id=\"id\" name=\"id\" value=\"" + id + "\">" +
                "    <input type=\"hidden\" id=\"mode\" name=\"mode\" value=\"suggest\">" +
                "    <input type=\"submit\" value=\"Submit\">\n" +
                "  </form>\n" +
                "</body>";

        outputStream.println(line);
    }

    @Override
    public void writeSuggestedUpdate(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        String uri = request.getRequestURI();
        String id = request.getParameter("id");
        ITransaction t = AbstractNode.getState().getSuggestedTransaction(id);
        Gson gson = new Gson();

        String line = "<!doctype html>\n" +
                "<html>\n" +
                "<head>\n" +
                "<meta charset=\"utf-8\">\n" +
                "<title>Client</title>\n" +
                "</head>\n" +
                "<body>\n" +
                "  <form action=\"" + uri.replace("api/update", "") + "\">\n" +
                "    <input type=\"submit\" value=\"Back\">\n" +
                "  </form><br/>\n" +
                "  <form action=\"" + uri + "\">\n" +
                "    <input type=\"hidden\" id=\"mode\" name=\"mode\" value=\"acc\">" +
                "    <input type=\"hidden\" id=\"id\" name=\"id\" value=\"" + id + "\">" +
                "    <input type=\"submit\" value=\"Accept\">\n" +
                "  </form><br/>\n" + gson.toJson(t) +
                "</body>";

        outputStream.println(line);
    }

    @Override
    public void writeQueryResults(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        String uri = request.getRequestURI().replace("api/sparql", "").replace("?", "");

        String line = "<!doctype html>\n" +
                "<html>\n" +
                "<head>\n" +
                "<meta charset=\"utf-8\">\n" +
                "<title>Client</title>\n" +
                "</head>\n" +
                "<body>\n" +
                "  <form action=\"" + uri.replace("api/update", "") + "\">\n" +
                "    <input type=\"submit\" value=\"Back\">\n" +
                "  </form><br/>\n";

        long timestamp = -1;
        if (request.getParameter("time") != null && !request.getParameter("time").equals(""))
            timestamp = Long.parseLong(request.getParameter("time"));

        String sparql = request.getParameter("query");
        Query query = QueryFactory.create(sparql);
        final ColchainGraph graph;
        if (timestamp == -1)
            graph = new ColchainGraph();
        else
            graph = new ColchainGraph(timestamp);
        Model model = ModelFactory.createModelForGraph(graph);

        final QueryExecution executor = QueryExecutionFactory.create(query, model);
        final ResultSet rs = executor.execSelect();

        while (rs.hasNext()) {
            QuerySolution sol = rs.next();
            String s = "";
            Iterator<String> vars = sol.varNames();
            while (vars.hasNext()) {
                String var = vars.next();
                s += var + " -> (" + sol.get(var).toString() + ") ";
            }

            line += s + "<br/>\n";
        }

        line += "</body>";

        outputStream.println(line);
    }
}
