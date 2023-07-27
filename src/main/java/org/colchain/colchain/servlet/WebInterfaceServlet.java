package org.colchain.colchain.servlet;

import jakarta.servlet.MultipartConfigElement;
import jakarta.servlet.annotation.MultipartConfig;
import jakarta.servlet.http.Part;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.colchain.colchain.node.AbstractNode;
import org.colchain.colchain.util.ConfigReader;
import org.colchain.colchain.writer.IResponseWriter;
import org.colchain.colchain.writer.ResponseWriterFactory;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.server.Request;
import org.linkeddatafragments.datasource.DataSourceTypesRegistry;
import org.linkeddatafragments.datasource.IDataSourceType;

import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Map;

@MultipartConfig
public class WebInterfaceServlet extends HttpServlet {
    private static final MultipartConfigElement MULTI_PART_CONFIG = new MultipartConfigElement("temp");
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

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String contentType = request.getContentType();
        IResponseWriter writer = ResponseWriterFactory.createWriter();
        if (contentType != null && contentType.startsWith("multipart/")) {
            request.setAttribute(Request.__MULTIPART_CONFIG_ELEMENT, MULTI_PART_CONFIG);
        } else {
            try {
                writer.writeInit(response.getOutputStream(), request);
            } catch (Exception e) {
                throw new ServletException(e);
            }
            return;
        }

        String mode = IOUtils.toString(request.getPart("mode").getInputStream(), Charset.defaultCharset());
        if (mode.equals("config")) {
            Part part = request.getPart("configfile");
            if (part == null || part.getSize() == 0) {
                try {
                    writer.writeInit(response.getOutputStream(), request);
                } catch (Exception e) {
                    throw new ServletException(e);
                }
                return;
            } else {
                String filename = part.getSubmittedFileName();
                InputStream is = part.getInputStream();
                FileUtils.copyInputStreamToFile(is, new File(filename));
                initConfig(filename);
            }
        } else if (mode.equals("read")) {
            Part part = request.getPart("statefile");
            if (part == null || part.getSize() == 0) {
                try {
                    writer.writeInit(response.getOutputStream(), request);
                } catch (Exception e) {
                    throw new ServletException(e);
                }
                return;
            } else {
                String filename = part.getSubmittedFileName();
                InputStream is = part.getInputStream();
                FileUtils.copyInputStreamToFile(is, new File(filename));
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
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    /**
     * @param request
     * @param response
     * @throws ServletException
     */
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        IResponseWriter writer = ResponseWriterFactory.createWriter();

        if (INIT) {
            try {
                writer.writeLandingPage(response.getOutputStream(), request);
            } catch (Exception e) {
                throw new ServletException(e.getMessage());
            }
        } else {
            try {
                writer.writeInit(response.getOutputStream(), request);
            } catch (Exception e) {
                throw new ServletException(e);
            }
        }
    }

    private String getPrefix(String uri) {
        if (uri.endsWith("/")) {
            uri = uri.substring(0, uri.length() - 1);
        }
        int count = StringUtils.countMatches(uri, "/") - 2;
        if (count == 0) return "N/A";
        if (count <= 4) {
            if (count < 1) System.out.println(uri);
            return uri.substring(0, StringUtils.ordinalIndexOf(uri, "/", 3));
        }

        int no = 2 + (int) Math.ceil((double) count / 2.0);

        int index = StringUtils.ordinalIndexOf(uri, "/", no);

        try {
            return uri.substring(0, index);
        } catch (StringIndexOutOfBoundsException e) {
            return uri.substring(0, uri.lastIndexOf("/"));
        }
    }
}
