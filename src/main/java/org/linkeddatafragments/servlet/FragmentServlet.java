package org.linkeddatafragments.servlet;

import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHeaders;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.LangBuilder;
import org.colchain.colchain.node.AbstractNode;
import org.colchain.colchain.servlet.WebInterfaceServlet;
import org.colchain.colchain.util.ConfigReader;
import org.colchain.colchain.writer.IResponseWriter;
import org.colchain.colchain.writer.ResponseWriterFactory;
import org.colchain.index.util.Tuple;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.hdt.HdtDataSource;
import org.linkeddatafragments.exceptions.DataSourceNotFoundException;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.util.MIMEParse;
import org.rdfhdt.hdt.hdt.HDT;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class FragmentServlet extends HttpServlet {
    public static ConfigReader config;
    private final Collection<String> mimeTypes = new ArrayList<>();
    public final static String CFGFILE = "configFile";

    private static Lang HDT_TYPE = LangBuilder.create("HDT", "text/hdt").addFileExtensions("hdt").build();

    private File getConfigFile(ServletConfig config) throws IOException {
        String path = config.getServletContext().getRealPath("/");

        if (path == null) {
            // this can happen when running standalone
            path = System.getProperty("user.dir");
        }
        File cfg = new File("config.json");
        if (config.getInitParameter(CFGFILE) != null) {
            cfg = new File(config.getInitParameter(CFGFILE));
        }
        if (!cfg.exists()) {
            throw new IOException("Configuration file " + cfg + " not found.");
        }
        if (!cfg.isFile()) {
            throw new IOException("Configuration file " + cfg + " is not a file.");
        }
        return cfg;
    }

    /**
     * @param servletConfig
     * @throws ServletException
     */
    @Override
    public void init(ServletConfig servletConfig) throws ServletException {
        try {
            // load the configuration
            File configFile = getConfigFile(servletConfig);
            config = new ConfigReader(new FileReader(configFile));
            MIMEParse.register(HDT_TYPE.getHeaderString());
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    /**
     *
     */
    @Override
    public void destroy() {

    }

    /**
     * Get the datasource
     *
     * @param request
     * @return
     * @throws DataSourceNotFoundException
     */
    private IDataSource getDataSource(HttpServletRequest request) throws DataSourceNotFoundException {
        String contextPath = request.getContextPath();
        String requestURI = request.getRequestURI();

        String path = contextPath == null
                ? requestURI
                : requestURI.substring(contextPath.length());

        String dataSourceName = path.substring(path.lastIndexOf("/") + 1).replace(".index.v1-1", "").replace(".hdt", "");
        IDataSource dataSource;
        if (request.getParameter("time") != null)
            dataSource = AbstractNode.getState().getDatasource(dataSourceName, Long.parseLong(request.getParameter("time")));
        else dataSource = AbstractNode.getState().getDatasource(dataSourceName);

        if (dataSource == null) {
            throw new DataSourceNotFoundException(dataSourceName);
        }
        return dataSource;
    }

    /**
     * @param request
     * @param response
     * @throws ServletException
     */
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException {
        if (!WebInterfaceServlet.INIT) {
            try {
                IResponseWriter rWriter = ResponseWriterFactory.createWriter();
                rWriter.writeNotInitiated(response.getOutputStream(), request);
            } catch (Exception e) {
                throw new ServletException(e);
            }
            return;
        }
        ILinkedDataFragment fragment = null;
        try {
            String acceptHeader = request.getHeader(HttpHeaders.ACCEPT);
            String bestMatch = MIMEParse.bestMatch(acceptHeader);
            response.setHeader(HttpHeaders.SERVER, "Linked Data Fragments Server");
            response.setContentType(bestMatch);
            response.setCharacterEncoding(StandardCharsets.UTF_8.name());
            String fileSource = "";

            try {
                final IDataSource dataSource = getDataSource(request);
                fileSource = ((HdtDataSource) dataSource).getFile();
                if (request.getRequestURI().contains(".hdt.index.v1-1")) fileSource += ".index.v1-1";
                else if (!request.getRequestURI().contains(".hdt")) throw new Exception("Unknown file type.");

                File file = new File(fileSource);

                FileUtils.copyFile(file, response.getOutputStream());
                response.getOutputStream().flush();
                response.getOutputStream().close();
            } catch (Exception e) {
                e.printStackTrace();
                throw new ServletException(e);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new ServletException(e);
        }
    }
}
