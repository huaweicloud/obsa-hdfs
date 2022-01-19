/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.server.http;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeErrorException;
import javax.management.RuntimeMBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.TabularData;

public class JMXHttpHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(JMXHttpHandler.class);

    protected transient MBeanServer mBeanServer = null;

    private ExecutorService threadPool = Executors.newFixedThreadPool(4);

    public JMXHttpHandler() {
        mBeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    @Override
    public void handle(final HttpExchange httpExchange) {
        this.threadPool.execute(new Runnable() {
            @Override
            public void run() {
                OutputStream out = null;
                JsonGenerator jg = null;
                try {
                    Map<String, String> queries = new HashMap<String, String>();
                    String rawQuery = httpExchange.getRequestURI().getRawQuery();
                    if (null != rawQuery) {
                        String[] params = rawQuery.split("&");
                        for (String param : params) {
                            String[] paras = param.split("=");
                            if ("get".equals(paras[0])) {
                                queries.put("get", param.substring(4));
                            } else if ("qry".equals(paras[0])) {
                                queries.put("qry", param.substring(4));
                            }
                        }
                    }

                    Headers responseHeaders = httpExchange.getResponseHeaders();
                    responseHeaders.set("Content-Type", "application/json; charset=utf8");
                    httpExchange.sendResponseHeaders(200, 0);

                    out = httpExchange.getResponseBody();
                    JsonFactory jsonFactory = new JsonFactory();
                    jg = jsonFactory.createJsonGenerator(out);
                    jg.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
                    jg.useDefaultPrettyPrinter();
                    jg.writeStartObject();

                    if (mBeanServer == null) {
                        jg.writeStringField("result", "ERROR");
                        jg.writeStringField("message", "No MBeanServer could be found");
                        jg.close();
                        LOG.error("No MBeanServer could be found.");
                        httpExchange.sendResponseHeaders(404, 0);
                    }

                    // query per mbean attribute
                    String getmethod = queries.get("get");
                    if (getmethod != null) {
                        String[] splitStrings = getmethod.split("\\:\\:");
                        if (splitStrings.length != 2) {
                            jg.writeStringField("result", "ERROR");
                            jg.writeStringField("message", "query format is not as expected.");
                            jg.close();
                            httpExchange.sendResponseHeaders(400, 0);
                        }
                        listBeans(jg, new ObjectName(splitStrings[0]), splitStrings[1], httpExchange);
                        jg.close();
                    }

                    // query per mbean
                    String qry = queries.get("qry");
                    if (qry == null) {
                        qry = "*:*";
                    }
                    listBeans(jg, new ObjectName(qry), null, httpExchange);
                } catch (IOException | MalformedObjectNameException e) {
                    LOG.error("caught an exception while processing JMX request", e);
                    try {
                        httpExchange.sendResponseHeaders(500, 0);
                    } catch (IOException e1) {
                        LOG.error("caught an exception while processing JMX request", e1);
                    }
                } finally {
                    try {
                        if (jg != null) {
                            jg.close();
                        }
                        if (out != null) {
                            out.close();
                        }
                    } catch (IOException e) {
                        LOG.error("caught an exception while processing JMX request", e);
                    }
                }
            }
        });
    }

    private void listBeans(JsonGenerator jg, ObjectName qry, String attribute, HttpExchange response)
        throws IOException {
        LOG.debug("Listing beans for " + qry);
        Set<ObjectName> names = null;
        names = mBeanServer.queryNames(qry, null);

        jg.writeArrayFieldStart("beans");
        Iterator<ObjectName> it = names.iterator();
        while (it.hasNext()) {
            ObjectName oname = it.next();
            MBeanInfo minfo;
            String code = "";
            Object attributeinfo = null;
            try {
                minfo = mBeanServer.getMBeanInfo(oname);
                code = minfo.getClassName();
                String prs = "";
                try {
                    if ("org.apache.commons.modeler.BaseModelMBean".equals(code)) {
                        prs = "modelerType";
                        code = (String) mBeanServer.getAttribute(oname, prs);
                    }
                    if (attribute != null) {
                        prs = attribute;
                        attributeinfo = mBeanServer.getAttribute(oname, prs);
                    }
                } catch (AttributeNotFoundException e) {
                    // If the modelerType attribute was not found, the class name is used
                    // instead.
                    LOG.error("getting attribute " + prs + " of " + oname
                        + " threw an exception", e);
                } catch (MBeanException e) {
                    // The code inside the attribute getter threw an exception so log it,
                    // and fall back on the class name
                    LOG.error("getting attribute " + prs + " of " + oname
                        + " threw an exception", e);
                } catch (RuntimeException e) {
                    // For some reason even with an MBeanException available to them
                    // Runtime exceptionscan still find their way through, so treat them
                    // the same as MBeanException
                    LOG.error("getting attribute " + prs + " of " + oname
                        + " threw an exception", e);
                } catch (ReflectionException e) {
                    // This happens when the code inside the JMX bean (setter?? from the
                    // java docs) threw an exception, so log it and fall back on the
                    // class name
                    LOG.error("getting attribute " + prs + " of " + oname
                        + " threw an exception", e);
                }
            } catch (InstanceNotFoundException e) {
                //Ignored for some reason the bean was not found so don't output it
                continue;
            } catch (IntrospectionException e) {
                // This is an internal error, something odd happened with reflection so
                // log it and don't output the bean.
                LOG.error("Problem while trying to process JMX query: " + qry
                    + " with MBean " + oname, e);
                continue;
            } catch (ReflectionException e) {
                // This happens when the code inside the JMX bean threw an exception, so
                // log it and don't output the bean.
                LOG.error("Problem while trying to process JMX query: " + qry
                    + " with MBean " + oname, e);
                continue;
            }

            jg.writeStartObject();
            jg.writeStringField("name", oname.toString());

            jg.writeStringField("modelerType", code);
            if (attribute != null && attributeinfo == null) {
                jg.writeStringField("result", "ERROR");
                jg.writeStringField("message", "No attribute with name " + attribute
                    + " was found.");
                jg.writeEndObject();
                jg.writeEndArray();
                jg.close();
                response.sendResponseHeaders(404, 0);
                return;
            }

            if (attribute != null) {
                writeAttribute(jg, attribute, attributeinfo);
            } else {
                MBeanAttributeInfo[] attrs = minfo.getAttributes();
                for (int i = 0; i < attrs.length; i++) {
                    writeAttribute(jg, oname, attrs[i]);
                }
            }
            jg.writeEndObject();
        }
        jg.writeEndArray();
    }

    private void writeAttribute(JsonGenerator jg, ObjectName oname, MBeanAttributeInfo attr) throws IOException {
        if (!attr.isReadable()) {
            return;
        }
        String attName = attr.getName();
        if ("modelerType".equals(attName)) {
            return;
        }
        if (attName.indexOf("=") >= 0 || attName.indexOf(":") >= 0
            || attName.indexOf(" ") >= 0) {
            return;
        }
        Object value = null;
        try {
            value = mBeanServer.getAttribute(oname, attName);
        } catch (RuntimeMBeanException e) {
            // UnsupportedOperationExceptions happen in the normal course of business,
            // so no need to log them as errors all the time.
            if (e.getCause() instanceof UnsupportedOperationException) {
                LOG.debug("getting attribute " + attName + " of " + oname + " threw an exception", e);
            } else {
                LOG.error("getting attribute " + attName + " of " + oname + " threw an exception", e);
            }
            return;
        } catch (RuntimeErrorException e) {
            // RuntimeErrorException happens when an unexpected failure occurs in getAttribute
            // for example https://issues.apache.org/jira/browse/DAEMON-120
            LOG.debug("getting attribute " + attName + " of " + oname + " threw an exception", e);
            return;
        } catch (AttributeNotFoundException e) {
            //Ignored the attribute was not found, which should never happen because the bean
            //just told us that it has this attribute, but if this happens just don't output
            //the attribute.
            return;
        } catch (MBeanException e) {
            //The code inside the attribute getter threw an exception so log it, and
            // skip outputting the attribute
            LOG.error("getting attribute " + attName + " of " + oname + " threw an exception", e);
            return;
        } catch (RuntimeException e) {
            //For some reason even with an MBeanException available to them Runtime exceptions
            //can still find their way through, so treat them the same as MBeanException
            LOG.error("getting attribute " + attName + " of " + oname + " threw an exception", e);
            return;
        } catch (ReflectionException e) {
            //This happens when the code inside the JMX bean (setter?? from the java docs)
            //threw an exception, so log it and skip outputting the attribute
            LOG.error("getting attribute " + attName + " of " + oname + " threw an exception", e);
            return;
        } catch (InstanceNotFoundException e) {
            //Ignored the mbean itself was not found, which should never happen because we
            //just accessed it (perhaps something unregistered in-between) but if this
            //happens just don't output the attribute.
            return;
        }

        writeAttribute(jg, attName, value);
    }

    private void writeAttribute(JsonGenerator jg, String attName, Object value) throws IOException {
        jg.writeFieldName(attName);
        writeObject(jg, value);
    }

    private void writeObject(JsonGenerator jg, Object value) throws IOException {
        if (value == null) {
            jg.writeNull();
        } else {
            Class<?> c = value.getClass();
            if (c.isArray()) {
                jg.writeStartArray();
                int len = Array.getLength(value);
                for (int j = 0; j < len; j++) {
                    Object item = Array.get(value, j);
                    writeObject(jg, item);
                }
                jg.writeEndArray();
            } else if (value instanceof Number) {
                Number n = (Number) value;
                jg.writeNumber(n.toString());
            } else if (value instanceof Boolean) {
                Boolean b = (Boolean) value;
                jg.writeBoolean(b);
            } else if (value instanceof CompositeData) {
                CompositeData cds = (CompositeData) value;
                CompositeType comp = cds.getCompositeType();
                Set<String> keys = comp.keySet();
                jg.writeStartObject();
                for (String key : keys) {
                    writeAttribute(jg, key, cds.get(key));
                }
                jg.writeEndObject();
            } else if (value instanceof TabularData) {
                TabularData tds = (TabularData) value;
                jg.writeStartArray();
                for (Object entry : tds.values()) {
                    writeObject(jg, entry);
                }
                jg.writeEndArray();
            } else {
                jg.writeString(value.toString());
            }
        }
    }
}
