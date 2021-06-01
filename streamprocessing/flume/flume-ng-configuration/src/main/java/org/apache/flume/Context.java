/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * The context is a key-value store used to pass configuration information
 * throughout the system.
 */
public class Context {

  private Map<String, String> parameters;

  public Context() {
    parameters = Collections.synchronizedMap(new HashMap<String, String>());
  }

  public Context(Map<String, String> paramters) {
    this();
    this.putAll(paramters);
  }

  /**
   * Gets a copy of the backing map structure.
   * @return immutable copy of backing map structure
   */
  public Map<String, String> getParameters() {
    synchronized (parameters) {

      return ImmutableMap.copyOf(parameters);
    }
  }
  /**
   * Removes all of the mappings from this map.
   */
  public void clear() {
    parameters.clear();
  }

  /**
   * Get properties which start with a prefix. When a property is returned,
   * the prefix is removed the from name. For example, if this method is
   * called with a parameter &quot;hdfs.&quot; and the context contains:
   * <code>
   * { hdfs.key = value, otherKey = otherValue }
   * </code>
   * this method will return a map containing:
   * <code>
   * { key = value}
   * </code>
   *
   * <b>Note:</b> The <tt>prefix</tt> must end with a period character. If not
   * this method will raise an IllegalArgumentException.
   *
   * @param prefix key prefix to find and remove from keys in resulting map
   * @return map with keys which matched prefix with prefix removed from
   *   keys in resulting map. If no keys are matched, the returned map is
   *   empty
   * @throws IllegalArguemntException if the given prefix does not end with
   *   a period character.
   */
  public Map<String, String> getSubProperties(String prefix) {
    Preconditions.checkArgument(prefix.endsWith("."),
        "The given prefix does not end with a period (" + prefix + ")");
    Map<String, String> result = Maps.newHashMap();
    synchronized (parameters) {
      for (Entry<String, String> entry : parameters.entrySet()) {
        String key = entry.getKey();
        if (key.startsWith(prefix)) {
          String name = key.substring(prefix.length());
          result.put(name, entry.getValue());
        }
      }
    }
    return ImmutableMap.copyOf(result);
  }
  /**
   * Associates all of the given map's keys and values in the Context.
   */
  public void putAll(Map<String, String> map) {
    parameters.putAll(map);
  }
  /**
   * Associates the specified value with the specified key in this context.
   * If the context previously contained a mapping for the key, the old value
   * is replaced by the specified value.
   * @param key key with which the specified value is to be associated
   * @param value to be associated with the specified key
   */
  public void put(String key, String value) {
    parameters.put(key, value);
  }

  /**
   * Returns true if this Context contains a mapping for key.
   * Otherwise, returns false.
   */
  public boolean containsKey(String key) {
    return parameters.containsKey(key);
  }

  /**
   * Gets value mapped to key, returning defaultValue if unmapped.
   * @param key to be found
   * @param defaultValue returned if key is unmapped
   * @return value associated with key
   */
  public Boolean getBoolean(String key, Boolean defaultValue) {
    String value = get(key);
    if (value != null) {
      return Boolean.valueOf(Boolean.parseBoolean(value.trim()));
    }
    return defaultValue;
  }
  /**
   * Gets value mapped to key, returning null if unmapped.
   * <p>
   * Note that this method returns an object as opposed to a
   * primitive. The configuration key requested may not be mapped
   * to a value and by returning the primitive object wrapper we can
   * return null. If the key does not exist the return value of
   * this method is assigned directly to a primitive, a
   * {@link NullPointerException} will be thrown.
   * </p>
   * @param key to be found
   * @return value associated with key or null if unmapped
   */
  public Boolean getBoolean(String key) {
    return getBoolean(key, null);
  }
  /**
   * Gets value mapped to key, returning defaultValue if unmapped.
   * @param key to be found
   * @param defaultValue returned if key is unmapped
   * @return value associated with key
   */
  public Integer getInteger(String key, Integer defaultValue) {
    String value = get(key);
    if (value != null) {
      return Integer.valueOf(Integer.parseInt(value.trim()));
    }
    return defaultValue;
  }
  /**
   * Gets value mapped to key, returning null if unmapped.
   * <p>
   * Note that this method returns an object as opposed to a
   * primitive. The configuration key requested may not be mapped
   * to a value and by returning the primitive object wrapper we can
   * return null. If the key does not exist the return value of
   * this method is assigned directly to a primitive, a
   * {@link NullPointerException} will be thrown.
   * </p>
   * @param key to be found
   * @return value associated with key or null if unmapped
   */
  public Integer getInteger(String key) {
    return getInteger(key, null);
  }
  /**
   * Gets value mapped to key, returning defaultValue if unmapped.
   * @param key to be found
   * @param defaultValue returned if key is unmapped
   * @return value associated with key
   */
  public Long getLong(String key, Long defaultValue) {
    String value = get(key);
    if (value != null) {
      return Long.valueOf(Long.parseLong(value.trim()));
    }
    return defaultValue;
  }
  /**
   * Gets value mapped to key, returning null if unmapped.
   * <p>
   * Note that this method returns an object as opposed to a
   * primitive. The configuration key requested may not be mapped
   * to a value and by returning the primitive object wrapper we can
   * return null. If the key does not exist the return value of
   * this method is assigned directly to a primitive, a
   * {@link NullPointerException} will be thrown.
   * </p>
   * @param key to be found
   * @return value associated with key or null if unmapped
   */
  public Long getLong(String key) {
    return getLong(key, null);
  }
  /**
   * Gets value mapped to key, returning defaultValue if unmapped.
   * @param key to be found
   * @param defaultValue returned if key is unmapped
   * @return value associated with key
   */
  public String getString(String key, String defaultValue) {
    return get(key, defaultValue);
  }
  /**
   * Gets value mapped to key, returning null if unmapped.
   * @param key to be found
   * @return value associated with key or null if unmapped
   */
  public String getString(String key) {
    return get(key);
  }


  /**
   * Gets value mapped to key, returning defaultValue if unmapped.
   * @param key to be found
   * @param defaultValue returned if key is unmapped
   * @return value associated with key
   */
  public Float getFloat(String key, Float defaultValue) {
    String value = get(key);
    if (value != null) {
      return Float.parseFloat(value.trim());
    }
    return defaultValue;
  }
  /**
   * Gets value mapped to key, returning null if unmapped.
   * <p>
   * Note that this method returns an object as opposed to a
   * primitive. The configuration key requested may not be mapped
   * to a value and by returning the primitive object wrapper we can
   * return null. If the key does not exist the return value of
   * this method is assigned directly to a primitive, a
   * {@link NullPointerException} will be thrown.
   * </p>
   * @param key to be found
   * @return value associated with key or null if unmapped
   */
  public Float getFloat(String key) {
    return getFloat(key, null);
  }
  /**
   * Gets value mapped to key, returning defaultValue if unmapped.
   * @param key to be found
   * @param defaultValue returned if key is unmapped
   * @return value associated with key
   */
  public Double getDouble(String key, Double defaultValue) {
    String value = get(key);
    if (value != null) {
      return Double.parseDouble(value.trim());
    }
    return defaultValue;
  }
  /**
   * Gets value mapped to key, returning null if unmapped.
   * <p>
   * Note that this method returns an object as opposed to a
   * primitive. The configuration key requested may not be mapped
   * to a value and by returning the primitive object wrapper we can
   * return null. If the key does not exist the return value of
   * this method is assigned directly to a primitive, a
   * {@link NullPointerException} will be thrown.
   * </p>
   * @param key to be found
   * @return value associated with key or null if unmapped
   */
  public Double getDouble(String key) {
    return getDouble(key, null);
  }

  private String get(String key, String defaultValue) {
    String result = parameters.get(key);
    if (result != null) {
      return result;
    }
    return defaultValue;
  }
  private String get(String key) {
    return get(key, null);
  }
  @Override
  public String toString() {
    return "{ parameters:" + parameters + " }";
  }
}
