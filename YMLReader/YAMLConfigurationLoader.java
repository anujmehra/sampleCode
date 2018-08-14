package com.am.analytics.common.core.utils;

import java.io.IOException;
import java.io.InputStream;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * The Class YAMLConfigurationLoader: used in loading yml configuration.
 *
 * @param <T> the generic type
 */
@Component("yamlConfigLoader")
public class YAMLConfigurationLoader<T> {

    /**
     * Load yaml.
     *
     * @param resourcePath the resource path
     * @param typeParameterClass the type parameter class
     * @return the t
     */
    public T loadYaml(final String resourcePath, final Class<T> typeParameterClass) {

        final InputStream stream = YAMLConfigurationLoader.class.getClassLoader().getResourceAsStream(resourcePath);
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        T yamlMetadata = null;

        try {
            yamlMetadata = mapper.readValue(stream, typeParameterClass);

        } catch (IOException e) {
            throw new ExceptionInInitializerError("Error  Initializing the properties " + e);
        }

        return yamlMetadata;
    }

	
	//caller method
	    /**
     * Method is responsible to load the Yml Configuration file into DatabaseMetaDataMaster object for the respective input file
     * being processed.
     * 
     * @param inputFileTableMappingMap Map<String, String> - contains mapping of table name and corresponding .yml file
     * @param fileName String - name of input file being processed
     * @return databaseMetaDataMaster DatabaseMetaDataMaster
     */
    protected DatabaseMetaDataMaster loadYmlConfig(final Map<String, String> inputFileTableMappingMap, final String fileName) {

        return yamlConfigLoader.loadYaml(this.getResourceLocation() + inputFileTableMappingMap.get(fileName),
            DatabaseMetaDataMaster.class);
    }
}
