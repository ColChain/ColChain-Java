package org.colchain.colchain.servlet;

import org.colchain.colchain.node.AbstractNode;
import org.colchain.colchain.util.ConfigReader;
import org.colchain.colchain.writer.IResponseWriter;
import org.colchain.colchain.writer.ResponseWriterFactory;
import org.apache.commons.lang3.StringUtils;
import org.linkeddatafragments.datasource.DataSourceTypesRegistry;
import org.linkeddatafragments.datasource.IDataSourceType;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class WebInterfaceServlet extends HttpServlet {
    public static boolean INIT = false;

    public final static String CFGFILE = "configFile";
    private ConfigReader config;

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
    }

    private void initConfig(String configName) throws ServletException {
        try {
            // load the configuration
            File configFile = new File(configName);
            config = new ConfigReader(new FileReader(configFile));

            // register data source types
            for (Map.Entry<String, IDataSourceType> typeEntry : config.getDataSourceTypes().entrySet()) {
                DataSourceTypesRegistry.register(typeEntry.getKey(),
                        typeEntry.getValue());
            }
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
     *
     */
    @Override
    public void destroy() {
    }

    /**
     * @param request
     * @param response
     * @throws ServletException
     */
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        IResponseWriter writer = ResponseWriterFactory.createWriter();

        if(INIT) {
            try {
                writer.writeLandingPage(response.getOutputStream(), request);
            } catch (Exception e) {
                throw new ServletException(e.getMessage());
            }
        } else {
            String mode = request.getParameter("mode");
            if(mode == null) {
                try {
                    writer.writeInit(response.getOutputStream(), request);
                } catch (Exception e) {
                    throw new ServletException(e);
                }
                return;
            }
            else if(mode.equals("config")) {
                String configFile = request.getParameter("config");
                if (configFile == null) {
                    try {
                        writer.writeInit(response.getOutputStream(), request);
                    } catch (Exception e) {
                        throw new ServletException(e);
                    }
                    return;
                } else {
                    // Config specified
                    initConfig(configFile);
                }
            } else if (mode.equals("read")) {
                String filename = request.getParameter("filename");
                if(filename == null) {
                    try {
                        writer.writeInit(response.getOutputStream(), request);
                    } catch (Exception e) {
                        throw new ServletException(e);
                    }
                    return;
                } else {
                    AbstractNode.loadState(filename);
                }
            } else {
                try {
                    writer.writeInit(response.getOutputStream(), request);
                } catch (Exception e) {
                    throw new ServletException(e);
                }
                return;
            }

            INIT = true;
            try {
                writer.writeRedirect(response.getOutputStream(), request, "");
            } catch(Exception e)  {
                throw new ServletException(e);
            }
        }
    }

    private String getPrefix(String uri) {
        if(uri.endsWith("/")) {
            uri = uri.substring(0, uri.length()-1);
        }
        int count = StringUtils.countMatches(uri, "/")-2;
        if(count == 0) return "N/A";
        if(count <= 4) {
            if(count < 1) System.out.println(uri);
            return uri.substring(0, StringUtils.ordinalIndexOf(uri, "/", 3));
        }

        int no = 2 + (int)Math.ceil((double)count / 2.0);

        int index = StringUtils.ordinalIndexOf(uri, "/", no);

        try {
            return uri.substring(0, index);
        } catch (StringIndexOutOfBoundsException e) {
            return uri.substring(0, uri.lastIndexOf("/"));
        }
    }
}
