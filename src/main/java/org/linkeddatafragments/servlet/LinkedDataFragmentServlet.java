package org.linkeddatafragments.servlet;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.colchain.colchain.node.AbstractNode;
import org.colchain.colchain.servlet.WebInterfaceServlet;
import org.colchain.colchain.writer.IResponseWriter;
import org.colchain.colchain.writer.ResponseWriterFactory;
import org.apache.http.HttpHeaders;
import org.apache.jena.riot.Lang;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.exceptions.DataSourceNotFoundException;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.util.MIMEParse;
import org.linkeddatafragments.views.ILinkedDataFragmentWriter;
import org.linkeddatafragments.views.LinkedDataFragmentWriterFactory;


public class LinkedDataFragmentServlet extends HttpServlet {
    private final static long serialVersionUID = 1L;


    public final static String CFGFILE = "configFile";
    private ConfigReader config;
    private final Collection<String> mimeTypes = new ArrayList<>();

    private File getConfigFile(ServletConfig config) throws IOException {
        String path = config.getServletContext().getRealPath("/");


        if (path == null) {
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
            MIMEParse.register(Lang.TTL.getHeaderString());
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
     * @throws IOException
     */
    private IDataSource getDataSource(HttpServletRequest request) throws DataSourceNotFoundException {
        String contextPath = request.getContextPath();
        String requestURI = request.getRequestURI();

        String path = contextPath == null
                ? requestURI
                : requestURI.substring(contextPath.length());

        String dataSourceName = path.substring(path.lastIndexOf("/")+1);
        IDataSource dataSource;
        if(request.getParameter("time") != null) dataSource = AbstractNode.getState().getDatasource(dataSourceName, Long.parseLong(request.getParameter("time")));
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
        if(!WebInterfaceServlet.INIT) {
            try {
                IResponseWriter rWriter = ResponseWriterFactory.createWriter();
                rWriter.writeNotInitiated(response.getOutputStream(), request);
            } catch(Exception e)  {
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

            ILinkedDataFragmentWriter writer = LinkedDataFragmentWriterFactory.create(config.getPrefixes(), AbstractNode.getState().getDatasources(), bestMatch);

            try {

                final IDataSource dataSource = getDataSource(request);

                final ILinkedDataFragmentRequest ldfRequest;

                if (request.getParameter("values") == null) {
                    ldfRequest = dataSource.getRequestParser(IDataSource.ProcessorType.TPF)
                            .parseIntoFragmentRequest(request, config);
                    fragment = dataSource.getRequestProcessor(IDataSource.ProcessorType.TPF)
                            .createRequestedFragment(ldfRequest);
                } else {
                    ldfRequest = dataSource.getRequestParser(IDataSource.ProcessorType.BRTPF)
                            .parseIntoFragmentRequest(request, config);
                    fragment = dataSource.getRequestProcessor(IDataSource.ProcessorType.BRTPF)
                            .createRequestedFragment(ldfRequest);
                }

                writer.writeFragment(response.getOutputStream(), dataSource, fragment, ldfRequest);
            } catch (DataSourceNotFoundException ex) {
                try {
                    response.setStatus(404);
                    writer.writeNotFound(response.getOutputStream(), request);
                } catch (Exception ex1) {
                    throw new ServletException(ex1);
                }
            } catch (Exception e) {
                e.printStackTrace();
                response.setStatus(500);
                writer.writeError(response.getOutputStream(), e);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new ServletException(e);
        } finally {
            // close the fragment
            if (fragment != null) {
                try {
                    fragment.close();
                } catch (Exception e) {
                    // ignore
                }
            }

        }
    }
}

