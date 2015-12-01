package com.alibaba.jstorm.simple;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author von gosling
 */
public class InnerLogger {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private InnerLogger() {
  }

  private static InnerLogger instance = new InnerLogger();

  public static InnerLogger getInstance() {
    return instance;
  }

  private static final String LOG4J_FACTORY = "org.slf4j.impl.Log4jLoggerFactory";
  private static final String LOG4J_CONFIG_XML = "org.apache.log4j.xml.DOMConfigurator";
  private static final String LOG4J_CONFIG_PROP = "org.apache.log4j.PropertyConfigurator";
  private static final String LOGBACK_FACTORY = "ch.qos.logback.classic.LoggerContext";

  public void resetLogger(@SuppressWarnings("rawtypes") Map conf) {
    String logFile = (String) conf.get("storm.client.logger");
    if (StringUtils.isBlank(logFile)) {
      logger.error("Null value for storm.client.logger configuration item !");
      return;
    }
    try {
      ILoggerFactory iLoggerFactory = LoggerFactory.getILoggerFactory();
      Class<?> classType = iLoggerFactory.getClass();
      if (LOG4J_FACTORY.equals(classType.getName())) {
        //LogManager.resetConfiguration();
        Class<?> logManagerCls = Class.forName("org.apache.log4j.LogManager");
        Method resetMethod = logManagerCls.newInstance().getClass()
            .getMethod("resetConfiguration");
        resetMethod.invoke(logManagerCls.newInstance());

        //Configurator.configure("");
        Class<?> configurator = null;
        String nameSuffix = logFile.substring(logFile.lastIndexOf(".") + 1);

        if ("xml".equals(nameSuffix)) {
          configurator = Class.forName(LOG4J_CONFIG_XML);
        } else if ("properties".equals(nameSuffix)) {
          configurator = Class.forName(LOG4J_CONFIG_PROP);
        } else {
          logger.error("Bad value->{} for storm.client.logger configuration item !",
              logFile);
          return;
        }

        Method configureMethod = configurator.getMethod("configure", URL.class);
        URL url = Thread.currentThread().getContextClassLoader().getResource(logFile);
        configureMethod.invoke(configurator.newInstance(), url);
      } else if (LOGBACK_FACTORY.equals(classType.getName())) {
        //configurator.setContext(context);
        Class<?> context = Class.forName("ch.qos.logback.core.Context");
        Object contextObj = context.newInstance();
        Class<?> joranConfigurator = Class
            .forName("ch.qos.logback.classic.joran.JoranConfigurator");
        Object joranConfiguratoroObj = joranConfigurator.newInstance();
        Method setContext = joranConfiguratoroObj.getClass().getMethod("setContext",
            context);
        setContext.invoke(joranConfiguratoroObj, iLoggerFactory);

        //context.reset();
        Method reset = context.getMethod("reset");
        reset.invoke(contextObj);

        //configurator.doConfigure("logback.xml");
        URL url = Thread.currentThread().getContextClassLoader().getResource(logFile);
        Method doConfigure = joranConfiguratoroObj.getClass().getMethod("doConfigure",
            URL.class);
        doConfigure.invoke(joranConfiguratoroObj, url);
      }
    } catch (Exception e) {
      logger.error("Initing custom log configuration error !", e);
    }
  }
}
