package com.am.analytics.common.core.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * Utility class created to handle commonly used in Application Context.
 */
@Component
public class ApplicationContextUtil implements ApplicationContextAware {

    /** The application context. */
    private static ApplicationContext applicationContext;

    /*
     * (non-Javadoc)
     * @see
     * org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
     */
    @Override
    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {

        if (null == ApplicationContextUtil.applicationContext) {
            ApplicationContextUtil.applicationContext = applicationContext;
        }
    }

    /**
     * Gets the bean.
     *
     * @param <T> the generic type
     * @param clazz the clazz
     * @return the bean
     */
    public static <T> T getBean(final Class<T> clazz) {

        T bean = null;
        if (null != clazz) {
            bean = applicationContext.getBean(clazz);
        }
        return bean;
    }

    /**
     * Gets the bean.
     *
     * @param <T> the generic type
     * @param name the name
     * @return the bean
     */
    public static <T> T getBean(final String name) {

        T bean = null;
        if (null != name) {
            bean = (T) applicationContext.getBean(name);
        }
        return bean;
    }

    /**
     * Gets the bean.
     *
     * @param <T> the generic type
     * @param clazz the clazz
     * @param args the args
     * @return the bean
     */
    public static <T> T getBean(final Class<T> clazz, final Object... args) {

        T bean = null;
        if (null != clazz) {
            bean = applicationContext.getBean(clazz, args);
        }
        return bean;
    }

}
