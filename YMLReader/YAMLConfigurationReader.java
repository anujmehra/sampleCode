package com.am.analytics.common.core.utils;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.PostConstruct;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Bean for this class has been created in config class 'AnalyticsIntgCommonsConfig'. Purpose of this class to read yaml
 * configuration
 *
 * @param <T> the generic type
 */
public class YAMLConfigurationReader<T> implements ConfigReader<T> {

    /** The yaml metadata. */
    private T yamlMetadata;

    /** The resource path. */
    private final String resourcePath;

    /** The type parameter class. */
    private final Class<T> typeParameterClass;

    /**
     * Parameterized Constructor.
     *
     * @param resourcePath the resource path
     * @param typeParameterClass the type parameter class
     */
    public YAMLConfigurationReader(final String resourcePath, final Class<T> typeParameterClass) {

        this.typeParameterClass = typeParameterClass;
        this.resourcePath = resourcePath;

    }

    /**
     * Load yaml.
     */
    @PostConstruct
    public void loadYaml() {

        final InputStream stream = YAMLConfigurationReader.class.getClassLoader().getResourceAsStream(resourcePath);
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        try {
            yamlMetadata = mapper.readValue(stream, typeParameterClass);

        } catch (IOException e) {
            throw new ExceptionInInitializerError("Error  Initializing the properties " + e);
        }

    }

    /**
     * Gets the yaml configuration.
     *
     * @return the yaml configuration
     */
    public T getYamlConfiguration() {

        return yamlMetadata;
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.common.core.utils.ConfigReader#get()
     */
    @Override
    public T get() {

        return null;
    }

}
